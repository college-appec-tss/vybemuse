
require("dotenv").config();

const express = require("express");
const helmet = require("helmet");
const rateLimit = require("express-rate-limit");
const path = require("node:path");
const net = require("node:net");
const { Readable, Transform } = require("node:stream");
const { pipeline } = require("node:stream/promises");

const app = express();

const PORT = Number(process.env.PORT) || 3000;
const VIDFETCH_API = "https://api.vidfetch.dev";
const VIDFETCH_API_KEY = process.env.VIDFETCH_API_KEY;

const SITE_URL = (
  process.env.SITE_URL || "https://vybemuse.onrender.com"
).replace(/\/+$/, "");

const MAX_FILE_BYTES = 512 * 1024 * 1024;

app.disable("x-powered-by");
app.set("trust proxy", 1);

app.use(helmet({
  contentSecurityPolicy: {
    directives: {
      defaultSrc: ["'self'"],
      baseUri: ["'self'"],
      formAction: ["'self'"],
      frameAncestors: ["'none'"],
      objectSrc: ["'none'"],
      scriptSrc: ["'self'", "'unsafe-inline'"],
      styleSrc: [
        "'self'",
        "'unsafe-inline'",
        "https://fonts.googleapis.com"
      ],
      fontSrc: [
        "'self'",
        "https://fonts.gstatic.com",
        "data:"
      ],
      imgSrc: ["'self'", "data:", "https:"],
      connectSrc: ["'self'"],
      upgradeInsecureRequests: []
    }
  },
  crossOriginEmbedderPolicy: false
}));

app.use(express.json({ limit: "12kb" }));

app.use("/api", rateLimit({
  windowMs: 15 * 60 * 1000,
  limit: 40,
  standardHeaders: "draft-8",
  legacyHeaders: false,
  message: {
    success: false,
    message: "Too many requests. Please wait and try again."
  }
}));

const allowedFormats = new Set([
  "144p",
  "240p",
  "360p",
  "480p",
  "720p",
  "1080p",
  "1440p",
  "2160p",
  "mp3"
]);

const supportedHosts = new Set([
  "youtube.com",
  "www.youtube.com",
  "m.youtube.com",
  "youtu.be",
  "tiktok.com",
  "www.tiktok.com",
  "vm.tiktok.com",
  "facebook.com",
  "www.facebook.com",
  "fb.watch",
  "instagram.com",
  "www.instagram.com",
  "vimeo.com",
  "www.vimeo.com"
]);

function validateVideoUrl(value) {
  if (typeof value !== "string" || value.length > 2048) {
    return null;
  }

  try {
    const parsed = new URL(value.trim());
    const hostname = parsed.hostname.toLowerCase();

    if (parsed.protocol !== "https:") return null;
    if (!supportedHosts.has(hostname)) return null;
    if (parsed.username || parsed.password || parsed.port) return null;

    return parsed.href;
  } catch {
    return null;
  }
}

function publicErrorMessage(message, fallback) {
  const safe = typeof message === "string"
    ? message.trim()
    : "";

  return safe && safe.length < 300 ? safe : fallback;
}

async function vidfetch(endpoint, {
  method = "GET",
  body,
  timeoutMs = 45000
} = {}) {
  if (!VIDFETCH_API_KEY) {
    const error = new Error(
      "The media service is not configured. Please try again later."
    );

    error.status = 503;
    throw error;
  }

  const controller = new AbortController();
  const timeout = setTimeout(
    () => controller.abort(),
    timeoutMs
  );

  try {
    const response = await fetch(`${VIDFETCH_API}${endpoint}`, {
      method,
      redirect: "error",
      signal: controller.signal,
      headers: {
        Authorization: `Bearer ${VIDFETCH_API_KEY}`,
        Accept: "application/json",
        ...(body ? {
          "Content-Type": "application/json"
        } : {})
      },
      ...(body ? {
        body: JSON.stringify(body)
      } : {})
    });

    const raw = await response.text();

    let data = {};

    if (raw) {
      try {
        data = JSON.parse(raw);
      } catch {
        const error = new Error(
          "The media provider returned an unreadable response."
        );

        error.status = 502;
        throw error;
      }
    }

    if (!response.ok) {
      const error = new Error(
        publicErrorMessage(
          data.message || data.error || data.detail,
          `Media provider returned HTTP ${response.status}.`
        )
      );

      error.status = response.status === 429 ? 429 : 502;
      throw error;
    }

    return data;
  } catch (error) {
    if (error.name === "AbortError") {
      const timeoutError = new Error(
        "The media provider took too long to respond."
      );

      timeoutError.status = 504;
      throw timeoutError;
    }

    if (error.name === "TypeError") {
      const networkError = new Error(
        "Could not connect to the media provider."
      );

      networkError.status = 502;
      throw networkError;
    }

    throw error;
  } finally {
    clearTimeout(timeout);
  }
}

function sendApiError(res, error, fallback) {
  console.error("[API error]", error.message);

  return res.status(error.status || 502).json({
    success: false,
    message: publicErrorMessage(error.message, fallback)
  });
}

function unwrapProviderObject(payload) {
  return payload?.job ||
    payload?.data?.job ||
    payload?.data ||
    payload?.result ||
    payload ||
    {};
}

async function getProviderJob(jobId) {
  const payload = await vidfetch(
    `/v1/downloads/${encodeURIComponent(jobId)}`,
    {
      method: "GET",
      timeoutMs: 25000
    }
  );

  return unwrapProviderObject(payload);
}

function getDownloadUrl(job) {
  return job.download_url ||
    job.result?.download_url ||
    job.delivery?.download_url ||
    job.output?.download_url ||
    null;
}

function validateProviderDownloadUrl(value) {
  if (typeof value !== "string" || value.length > 8192) {
    return null;
  }

  try {
    const url = new URL(value);

    if (url.protocol !== "https:") return null;
    if (url.username || url.password || url.port) return null;

    const hostname = url.hostname.toLowerCase();

    if (
      hostname === "localhost" ||
      hostname.endsWith(".localhost") ||
      hostname.endsWith(".local") ||
      net.isIP(hostname)
    ) {
      return null;
    }

    return url.href;
  } catch {
    return null;
  }
}

function safeFilename(value, extension) {
  const cleaned = String(value || "vybemuse-download")
    .normalize("NFKD")
    .replace(/[^\w\s.-]/g, "")
    .trim()
    .replace(/[\s.]+/g, "-")
    .slice(0, 90);

  return `${cleaned || "vybemuse-download"}.${extension}`;
}

/* =========================
   WEBSITE AND SEO
========================= */

app.get("/", (_req, res) => {
  res.setHeader("Cache-Control", "no-cache");
  res.sendFile(path.join(__dirname, "index.html"));
});

app.get("/robots.txt", (_req, res) => {
  res.type("text/plain").send(
    [
      "User-agent: *",
      "Allow: /",
      "Disallow: /api/",
      `Sitemap: ${SITE_URL}/sitemap.xml`
    ].join("\n")
  );
});

app.get("/sitemap.xml", (_req, res) => {
  res.type("application/xml").send(`<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url>
    <loc>${SITE_URL}/</loc>
    <changefreq>weekly</changefreq>
    <priority>1.0</priority>
  </url>
</urlset>`);
});

/* Only expose explicitly approved static assets. */

app.get("/favicon.svg", (_req, res) => {
  res.sendFile(
    path.join(__dirname, "favicon.svg"),
    error => {
      if (error && !res.headersSent) {
        res.sendStatus(404);
      }
    }
  );
});

app.get("/og-cover.svg", (_req, res) => {
  res.sendFile(
    path.join(__dirname, "og-cover.svg"),
    error => {
      if (error && !res.headersSent) {
        res.sendStatus(404);
      }
    }
  );
});

/* =========================
   HEALTH CHECK
========================= */

app.get("/api/health", (_req, res) => {
  res.json({
    success: true,
    service: "Vybemuse",
    status: "online",
    mediaProviderConfigured: Boolean(VIDFETCH_API_KEY)
  });
});

/* =========================
   VIDEO INFORMATION
========================= */

app.post("/api/video/info", async (req, res) => {
  const url = validateVideoUrl(req.body?.url);

  if (!url) {
    return res.status(400).json({
      success: false,
      message: "Enter a valid HTTPS URL from a supported platform."
    });
  }

  try {
    const data = await vidfetch("/v1/info", {
      method: "POST",
      body: { url },
      timeoutMs: 45000
    });

    const video = data.video ||
      data.data?.video ||
      data.data ||
      data.result ||
      data;

    return res.json({
      success: true,
      video
    });
  } catch (error) {
    return sendApiError(
      res,
      error,
      "Could not retrieve video information."
    );
  }
});

/* =========================
   CREATE DOWNLOAD JOB
========================= */

app.post("/api/video/download", async (req, res) => {
  const url = validateVideoUrl(req.body?.url);

  const format = typeof req.body?.format === "string"
    ? req.body.format.toLowerCase()
    : "";

  if (!url) {
    return res.status(400).json({
      success: false,
      message: "Enter a valid HTTPS video URL."
    });
  }

  if (!allowedFormats.has(format)) {
    return res.status(400).json({
      success: false,
      message: "Choose a supported output format."
    });
  }

  if (req.body?.authorized !== true) {
    return res.status(403).json({
      success: false,
      message: "Confirm that you have permission to download this media."
    });
  }

  try {
    const data = await vidfetch("/v1/downloads", {
      method: "POST",
      body: { url, format },
      timeoutMs: 45000
    });

    const job = unwrapProviderObject(data);

    if (
      typeof job.id !== "string" ||
      !/^dl_[a-zA-Z0-9_-]{4,100}$/.test(job.id)
    ) {
      console.error(
        "[VidFetch] Unexpected create-job response:",
        JSON.stringify(data).slice(0, 1000)
      );

      return res.status(502).json({
        success: false,
        message: "The provider did not return a valid download job ID."
      });
    }

    return res.status(202).json({
      success: true,
      job: {
        id: job.id,
        status: job.status || "queued",
        format: job.format || format
      }
    });
  } catch (error) {
    return sendApiError(
      res,
      error,
      "Could not create the download job."
    );
  }
});

/* =========================
   CHECK DOWNLOAD STATUS
========================= */

app.get("/api/video/status/:id", async (req, res) => {
  const jobId = req.params.id;

  if (!/^dl_[a-zA-Z0-9_-]{4,100}$/.test(jobId)) {
    return res.status(400).json({
      success: false,
      message: "Invalid download job ID."
    });
  }

  try {
    const job = await getProviderJob(jobId);

    const status = String(job.status || "unknown").toLowerCase();

    const downloadUrl = validateProviderDownloadUrl(
      getDownloadUrl(job)
    );

    return res.json({
      success: true,
      job: {
        id: job.id || jobId,
        status,
        format: job.format || null,
        downloadReady: Boolean(downloadUrl),
        message: job.message || job.error || null
      }
    });
  } catch (error) {
    return sendApiError(
      res,
      error,
      "Could not check download progress."
    );
  }
});

/* =========================
   STREAM FILE THROUGH VYBEMUSE
========================= */

app.get("/api/video/file/:id", async (req, res) => {
  const jobId = req.params.id;

  if (!/^dl_[a-zA-Z0-9_-]{4,100}$/.test(jobId)) {
    return res.status(400).send("Invalid download job ID.");
  }

  let controller;

  try {
    const job = await getProviderJob(jobId);

    const status = String(job.status || "").toLowerCase();

    if (![
      "completed",
      "complete",
      "succeeded",
      "success"
    ].includes(status)) {
      return res.status(409).send(
        "This download is not ready yet."
      );
    }

    const downloadUrl = validateProviderDownloadUrl(
      getDownloadUrl(job)
    );

    if (!downloadUrl) {
      return res.status(502).send(
        "The provider did not return a valid secure download URL."
      );
    }

    controller = new AbortController();

    const upstream = await fetch(downloadUrl, {
      method: "GET",
      redirect: "error",
      signal: controller.signal,
      headers: {
        Accept: "application/octet-stream, video/*, audio/*, */*"
      }
    });

    if (!upstream.ok || !upstream.body) {
      return res.status(502).send(
        `The provider returned HTTP ${upstream.status}.`
      );
    }

    const contentLength = Number(
      upstream.headers.get("content-length") || 0
    );

    if (contentLength > MAX_FILE_BYTES) {
      controller.abort();

      return res.status(413).send(
        "The file is larger than the current 512 MB limit."
      );
    }

    const sourceType = upstream.headers.get("content-type") || "";

    if (
      sourceType.includes("text/html") ||
      sourceType.includes("application/json")
    ) {
      controller.abort();

      return res.status(502).send(
        "The provider returned a webpage instead of a media file."
      );
    }

    const extension = String(job.format || "").toLowerCase() === "mp3"
      ? "mp3"
      : "mp4";

    const filename = safeFilename(
      job.title || job.filename || `vybemuse-${jobId}`,
      extension
    );

    res.status(200);

    res.setHeader(
      "Content-Type",
      sourceType || (
        extension === "mp3"
          ? "audio/mpeg"
          : "video/mp4"
      )
    );

    res.setHeader(
      "Content-Disposition",
      `attachment; filename="${filename}"; filename*=UTF-8''${encodeURIComponent(filename)}`
    );

    res.setHeader("Cache-Control", "private, no-store");
    res.setHeader("X-Content-Type-Options", "nosniff");

    let bytesSent = 0;

    const byteLimit = new Transform({
      transform(chunk, _encoding, callback) {
        bytesSent += chunk.length;

        if (bytesSent > MAX_FILE_BYTES) {
          callback(
            new Error("File exceeded the 512 MB transfer limit.")
          );
          return;
        }

        callback(null, chunk);
      }
    });

    const timeout = setTimeout(
      () => controller.abort(),
      10 * 60 * 1000
    );

    try {
      await pipeline(
        Readable.fromWeb(upstream.body),
        byteLimit,
        res,
        { signal: controller.signal }
      );
    } finally {
      clearTimeout(timeout);
    }

    console.log(
      `[Download] ${jobId} streamed ${bytesSent} bytes`
    );
  } catch (error) {
    if (!res.headersSent) {
      console.error("[Download error]", error.message);

      return res.status(502).send(
        "The file could not be streamed. Please try again."
      );
    }

    console.error("[Stream interrupted]", error.message);
    res.destroy(error);
  }
});

/* =========================
   UNKNOWN API ROUTES
========================= */

app.use("/api", (_req, res) => {
  res.status(404).json({
    success: false,
    message: "API endpoint not found."
  });
});

/* =========================
   ERROR HANDLER
========================= */

app.use((error, _req, res, _next) => {
  if (
    error instanceof SyntaxError &&
    error.status === 400 &&
    "body" in error
  ) {
    return res.status(400).json({
      success: false,
      message: "Invalid JSON request."
    });
  }

  console.error("[Server error]", error.message);

  return res.status(500).json({
    success: false,
    message: "An unexpected server error occurred."
  });
});

/* =========================
   START SERVER
========================= */

app.listen(PORT, () => {
  console.log(`Vybemuse running on port ${PORT}`);

  console.log(
    `VidFetch API key: ${
      VIDFETCH_API_KEY ? "configured" : "missing"
    }`
  );

  console.log(`Website URL: ${SITE_URL}`);
});
