require("dotenv").config();

const express = require("express");
const helmet = require("helmet");
const rateLimit = require("express-rate-limit");
const path = require("node:path");
const { Readable, Transform } = require("node:stream");
const { pipeline } = require("node:stream/promises");
const net = require("node:net");

const app = express();

const PORT = Number(process.env.PORT) || 3000;
const VIDFETCH_API = "https://api.vidfetch.dev";
const VIDFETCH_API_KEY = process.env.VIDFETCH_API_KEY;
const SITE_URL = (
  process.env.SITE_URL || "https://vybemuse.onrender.com"
).replace(/\/+$/, "");

const MAX_FILE_BYTES = 512 * 1024 * 1024;
const JOB_ID_PATTERN = /^dl_[a-zA-Z0-9_-]{4,100}$/;

const ALLOWED_FORMATS = new Set([
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

const SUPPORTED_HOSTS = new Set([
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

function validateMediaUrl(value) {
  if (typeof value !== "string" || value.length > 2048) {
    return null;
  }

  try {
    const url = new URL(value.trim());
    const hostname = url.hostname.toLowerCase();

    if (url.protocol !== "https:") return null;
    if (!SUPPORTED_HOSTS.has(hostname)) return null;
    if (url.username || url.password || url.port) return null;

    return url.href;
  } catch {
    return null;
  }
}

function safeMessage(value, fallback) {
  if (typeof value === "string") {
    const message = value.trim();

    if (message && message.length <= 300) {
      return message;
    }
  }

  return fallback;
}

function extractProviderError(data, fallback) {
  const detail = data?.detail;
  const error = data?.error;

  if (typeof detail === "string") return detail;
  if (typeof error === "string") return error;

  return (
    detail?.message ||
    detail?.hint ||
    error?.message ||
    data?.message ||
    fallback
  );
}

async function vidfetch(endpoint, options = {}) {
  if (!VIDFETCH_API_KEY) {
    const error = new Error(
      "VidFetch is not configured. Check the VIDFETCH_API_KEY environment variable."
    );

    error.status = 503;
    throw error;
  }

  const method = options.method || "GET";
  const body = options.body;
  const timeoutMs = options.timeoutMs || 45000;

  const controller = new AbortController();
  const timeout = setTimeout(
    () => controller.abort(),
    timeoutMs
  );

  const startedAt = Date.now();

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
          "VidFetch returned an invalid response."
        );

        error.status = 502;
        throw error;
      }
    }

    if (!response.ok) {
      const message = extractProviderError(
        data,
        `VidFetch returned HTTP ${response.status}.`
      );

      const error = new Error(
        safeMessage(message, "VidFetch request failed.")
      );

      error.status = [400, 401, 402, 403, 404, 422, 429].includes(
        response.status
      )
        ? response.status
        : 502;

      error.providerStatus = response.status;
      throw error;
    }

    console.log(
      `[VidFetch] ${method} ${endpoint} -> ${response.status} (${Date.now() - startedAt}ms)`
    );

    return data;
  } catch (error) {
    if (error.name === "AbortError") {
      const timeoutError = new Error(
        "VidFetch took too long to respond. Please try again."
      );

      timeoutError.status = 504;
      throw timeoutError;
    }

    if (error.name === "TypeError") {
      const networkError = new Error(
        "Could not connect to VidFetch. Please try again later."
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
    message: safeMessage(error.message, fallback)
  });
}

function unwrapProviderObject(payload) {
  return (
    payload?.job ||
    payload?.data?.job ||
    payload?.data ||
    payload?.result ||
    payload ||
    {}
  );
}

async function getProviderJob(jobId) {
  const payload = await vidfetch(
    `/v1/downloads/${encodeURIComponent(jobId)}`,
    {
      method: "GET",
      timeoutMs: 1200000
    }
  );

  return unwrapProviderObject(payload);
}

function getDownloadUrl(job) {
  return (
    job?.download_url ||
    job?.result?.download_url ||
    job?.delivery?.download_url ||
    job?.output?.download_url ||
    null
  );
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

/* Website */

app.get("/", (_req, res) => {
  res.sendFile(path.join(__dirname, "index.html"));
});

app.use(express.static(__dirname, {
  index: false,
  maxAge: "7d",
  setHeaders(res, filePath) {
    if (filePath.endsWith(".html")) {
      res.setHeader("Cache-Control", "no-cache");
    }
  }
}));

/* SEO */

app.get("/robots.txt", (_req, res) => {
  res.type("text/plain").send(
    `User-agent: *\nAllow: /\nDisallow: /api/\nSitemap: ${SITE_URL}/sitemap.xml\n`
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

/* Health check */

app.get("/api/health", (_req, res) => {
  res.json({
    success: true,
    service: "Vybemuse",
    status: "online",
    mediaProviderConfigured: Boolean(VIDFETCH_API_KEY)
  });
});

/* Retrieve media information */

app.post("/api/video/info", async (req, res) => {
  const url = validateMediaUrl(req.body?.url);

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
      timeoutMs: 300000
    });

    const media = unwrapProviderObject(data);

    return res.json({
      success: true,
      video: media
    });
  } catch (error) {
    return sendApiError(
      res,
      error,
      "Could not retrieve media information."
    );
  }
});

/* Create a download job */

app.post("/api/video/download", async (req, res) => {
  const url = validateMediaUrl(req.body?.url);

  const format = typeof req.body?.format === "string"
    ? req.body.format.toLowerCase()
    : "720p";

  if (!url) {
    return res.status(400).json({
      success: false,
      message: "Enter a valid HTTPS URL from a supported platform."
    });
  }

  if (!ALLOWED_FORMATS.has(format)) {
    return res.status(422).json({
      success: false,
      message: "Choose one of the supported output formats."
    });
  }

  if (req.body?.authorized !== true) {
    return res.status(403).json({
      success: false,
      message: "Confirm that you own this media or have permission to download it."
    });
  }

  try {
    const data = await vidfetch("/v1/downloads", {
      method: "POST",
      body: {
        url,
        format,
        destination: "url"
      },
      timeoutMs: 60000
    });

    const job = unwrapProviderObject(data);

    if (
      typeof job.id !== "string" ||
      !JOB_ID_PATTERN.test(job.id)
    ) {
      console.error(
        "[VidFetch] Invalid create-job response:",
        JSON.stringify(data).slice(0, 1000)
      );

      return res.status(502).json({
        success: false,
        message: "VidFetch did not return a valid download job ID."
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

/* Check download progress */

app.get("/api/video/status/:id", async (req, res) => {
  const jobId = req.params.id;

  if (!JOB_ID_PATTERN.test(jobId)) {
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

    const providerError =
      job.error?.message ||
      job.error_message ||
      job.message ||
      null;

    return res.json({
      success: true,
      job: {
        id: job.id || jobId,
        status,
        format: job.format || null,
        title: job.title || null,
        progress: job.progress ?? null,
        downloadReady: status === "completed" && Boolean(downloadUrl),
        message: providerError
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

/* Stream the completed file through Vybemuse */

app.get("/api/video/file/:id", async (req, res) => {
  const jobId = req.params.id;

  if (!JOB_ID_PATTERN.test(jobId)) {
    return res.status(400).send("Invalid download job ID.");
  }

  let controller;
  let timeout;

  try {
    const job = await getProviderJob(jobId);
    const status = String(job.status || "").toLowerCase();

    if (status !== "completed") {
      return res.status(409).send(
        "This download is not completed yet."
      );
    }

    const downloadUrl = validateProviderDownloadUrl(
      getDownloadUrl(job)
    );

    if (!downloadUrl) {
      return res.status(502).send(
        "VidFetch did not provide a valid download URL. Check the job destination and status."
      );
    }

    controller = new AbortController();

    /*
     * This server-side request follows CDN redirects when necessary.
     * The visitor still downloads from the Vybemuse URL, not the CDN URL.
     */
    const upstream = await fetch(downloadUrl, {
      method: "GET",
      redirect: "follow",
      signal: controller.signal,
      headers: {
        Accept: "application/octet-stream, video/*, audio/*, */*"
      }
    });

    if (!upstream.ok || !upstream.body) {
      return res.status(502).send(
        `The file provider returned HTTP ${upstream.status}. Please retry.`
      );
    }

    const contentLength = Number(
      upstream.headers.get("content-length") || 0
    );

    if (contentLength > MAX_FILE_BYTES) {
      controller.abort();

      return res.status(413).send(
        "This file exceeds the current 512 MB transfer limit."
      );
    }

    const contentType = upstream.headers.get("content-type") || "";

    if (
      contentType.includes("text/html") ||
      contentType.includes("application/json")
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
      contentType || (
        extension === "mp3" ? "audio/mpeg" : "video/mp4"
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

    timeout = setTimeout(
      () => controller.abort(),
      10 * 60 * 1000
    );

    await pipeline(
      Readable.fromWeb(upstream.body),
      byteLimit,
      res,
      { signal: controller.signal }
    );

    console.log(
      `[Download] ${jobId}: ${bytesSent} bytes streamed`
    );
  } catch (error) {
    console.error("[Download stream error]", error.message);

    if (!res.headersSent) {
      return res.status(502).send(
        "The file could not be streamed. Please try again."
      );
    }

    res.destroy(error);
  } finally {
    if (timeout) clearTimeout(timeout);
  }
});

/* Unknown API routes */

app.use("/api", (_req, res) => {
  res.status(404).json({
    success: false,
    message: "API endpoint not found."
  });
});

/* Error handler */

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

app.listen(PORT, () => {
  console.log(`Vybemuse listening on port ${PORT}`);
  console.log(
    `VidFetch API key: ${VIDFETCH_API_KEY ? "configured" : "missing"}`
  );
});
