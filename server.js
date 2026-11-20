require("dotenv").config();

const express = require("express");
const helmet = require("helmet");
const rateLimit = require("express-rate-limit");
const path = require("path");
const crypto = require("node:crypto");
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
const INFO_TIMEOUT_MS = 10 * 60 * 1000;

const infoTasks = new Map();

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

// API rate limiting
app.use("/api", rateLimit({
  windowMs: 15 * 60 * 1000,
  limit: 500,
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

// Exact domains and their legitimate subdomains.
const supportedDomains = [
  "youtube.com",
  "tiktok.com",
  "facebook.com",
  "instagram.com",
  "vimeo.com"
];

// These short domains are accepted exactly.
const supportedExactHosts = new Set([
  "youtu.be",
  "fb.watch"
]);

function isSupportedHost(hostname) {
  if (supportedExactHosts.has(hostname)) {
    return true;
  }

  return supportedDomains.some((domain) =>
    hostname === domain || hostname.endsWith(`.${domain}`)
  );
}

function validateVideoUrl(value) {
  if (typeof value !== "string" || value.length > 2048) {
    return null;
  }

  try {
    const parsed = new URL(value.trim());
    const hostname = parsed.hostname.toLowerCase();

    if (parsed.protocol !== "https:") return null;
    if (!isSupportedHost(hostname)) return null;

    if (parsed.username || parsed.password || parsed.port) {
      return null;
    }

    return parsed.href;

  } catch {
    return null;
  }
}

function publicErrorMessage(message, fallback) {
  const safe = typeof message === "string" ? message.trim() : "";

  return safe && safe.length < 300 ? safe : fallback;
}

// VIDFETCH REQUEST
async function vidfetch(endpoint, {
  method = "GET",
  body,
  timeoutMs = INFO_TIMEOUT_MS
} = {}) {

  if (!VIDFETCH_API_KEY) {
    const error = new Error(
      "The media service is not configured yet."
    );

    error.status = 503;
    throw error;
  }

  const controller = new AbortController();

  const timeout = setTimeout(() => {
    controller.abort();
  }, timeoutMs);

  const startedAt = Date.now();

  try {
    const response = await fetch(
      `${VIDFETCH_API}${endpoint}`,
      {
        method,
        redirect: "error",
        signal: controller.signal,

        headers: {
          Authorization: `Bearer ${VIDFETCH_API_KEY}`,
          Accept: "application/json",

          ...(body
            ? { "Content-Type": "application/json" }
            : {})
        },

        ...(body
          ? { body: JSON.stringify(body) }
          : {})
      }
    );

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
          `videos returned ${response.status}  may be it is not a short video.`
        )
      );

      error.status = response.status === 429 ? 429 : 502;
      throw error;
    }

    console.log(
      `[VidFetch] ${method} ${endpoint} completed in ${
        Date.now() - startedAt
      }ms`
    );

    return data;

  } catch (error) {

    if (error.name === "AbortError") {
      const timeoutError = new Error(
        `VidFetch did not respond within ${
          Math.round(timeoutMs / 60000)
        } minutes.`
      );

      timeoutError.status = 504;
      throw timeoutError;
    }

    if (error.name === "TypeError") {
      const networkError = new Error(
        "Could not connect to the media provider. Please try again."
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
      timeoutMs: 25000
    }
  );

  return unwrapProviderObject(payload);
}

function getDownloadUrl(job) {
  return (
    job.download_url ||
    job.result?.download_url ||
    job.delivery?.download_url ||
    job.output?.download_url ||
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

    if (url.username || url.password || url.port) {
      return null;
    }

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

  const base = cleaned || "vybemuse-download";

  return `${base}.${extension}`;
}

// WEBSITE
app.get("/", (_req, res) => {
  res.sendFile(path.join(__dirname, "index.html"));
});

app.use(express.static(__dirname, {
  maxAge: "7d",
  index: false,

  setHeaders(res, filePath) {
    if (filePath.endsWith(".html")) {
      res.setHeader("Cache-Control", "no-cache");
    }
  }
}));

// SEO
app.get("/robots.txt", (_req, res) => {
  res.type("text/plain").send(
    `User-agent: *\nAllow: /\nDisallow: /api/\nSitemap: ${SITE_URL}/sitemap.xml\n`
  );
});

app.get("/sitemap.xml", (_req, res) => {
  res.type("application/xml").send(
    `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url>
    <loc>${SITE_URL}/</loc>
    <changefreq>weekly</changefreq>
    <priority>1.0</priority>
  </url>
</urlset>`
  );
});

// HEALTH
app.get("/api/health", (_req, res) => {
  res.json({
    success: true,
    service: "Vybemuse",
    status: "online",
    mediaProviderConfigured: Boolean(VIDFETCH_API_KEY)
  });
});

// MEDIA INFORMATION
app.post("/api/video/info", (req, res) => {
  const url = validateVideoUrl(req.body?.url);

  if (!url) {
    return res.status(400).json({
      success: false,
      message: "Enter a valid HTTPS media URL from a supported platform."
    });
  }

  if (!VIDFETCH_API_KEY) {
    return res.status(503).json({
      success: false,
      message: "The media service is not configured yet."
    });
  }

  const taskId =
    `info_${crypto.randomUUID().replace(/-/g, "")}`;

  infoTasks.set(taskId, {
    status: "processing",
    createdAt: Date.now(),
    video: null,
    message: null
  });

  // Run the provider request in the background.
  (async () => {
    try {
      const data = await vidfetch("/v1/info", {
        method: "POST",
        body: { url },
        timeoutMs: INFO_TIMEOUT_MS
      });

      const video =
        data.video ||
        data.data?.video ||
        data.data ||
        data.result ||
        data;

      infoTasks.set(taskId, {
        status: "completed",
        createdAt: Date.now(),
        video,
        message: null
      });

      console.log(`[Info task] ${taskId} completed`);

    } catch (error) {
      console.error("[VidFetch info task]", error.message);

      infoTasks.set(taskId, {
        status: "failed",
        createdAt: Date.now(),
        video: null,
        message: publicErrorMessage(
          error.message,
          "Could not retrieve media information."
        )
      });
    }
  })();

  // Remove old task after 20 minutes.
  const cleanup = setTimeout(() => {
    infoTasks.delete(taskId);
  }, 20 * 60 * 1000);

  cleanup.unref?.();

  return res.status(202).json({
    success: true,
    taskId,
    status: "processing",
    timeoutSeconds: INFO_TIMEOUT_MS / 1000
  });
});

// INFORMATION TASK STATUS
app.get("/api/video/info/status/:id", (req, res) => {
  const taskId = req.params.id;

  if (!/^info_[a-f0-9]{32}$/.test(taskId)) {
    return res.status(400).json({
      success: false,
      message: "Invalid information task ID."
    });
  }

  const task = infoTasks.get(taskId);

  if (!task) {
    return res.status(404).json({
      success: false,
      message: "This task expired or the server restarted. Please try again."
    });
  }

  if (task.status === "processing") {
    return res.json({
      success: true,
      task: {
        status: "processing"
      }
    });
  }

  if (task.status === "failed") {
    return res.json({
      success: false,
      task: {
        status: "failed"
      },
      message: task.message
    });
  }

  return res.json({
    success: true,
    task: {
      status: "completed",
      video: task.video
    }
  });
});

// CREATE DOWNLOAD JOB
app.post("/api/video/download", async (req, res) => {
  const url = validateVideoUrl(req.body?.url);

  const format = typeof req.body?.format === "string"
    ? req.body.format.toLowerCase()
    : "720p";

  if (!url) {
    return res.status(400).json({
      success: false,
      message: "Enter a valid HTTPS media URL from a supported platform."
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

      timeoutMs: 45000
    });

    const job = unwrapProviderObject(data);

    if (
      typeof job.id !== "string" ||
      !/^dl_[a-zA-Z0-9_-]{4,100}$/.test(job.id)
    ) {
      console.error(
        "[VidFetch] unexpected create-job response:",
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

// DOWNLOAD STATUS
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

    const status = String(
      job.status || "unknown"
    ).toLowerCase();

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

// STREAM FILE THROUGH VYBEMUSE
app.get("/api/video/file/:id", async (req, res) => {
  const jobId = req.params.id;

  if (!/^dl_[a-zA-Z0-9_-]{4,100}$/.test(jobId)) {
    return res.status(400).send("Invalid download job ID.");
  }

  let controller;

  try {
    const job = await getProviderJob(jobId);

    const status = String(
      job.status || ""
    ).toLowerCase();

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
        `The file provider returned HTTP ${upstream.status}. Please retry.`
      );
    }

    const contentLength = Number(
      upstream.headers.get("content-length") || 0
    );

    if (contentLength > MAX_FILE_BYTES) {
      controller.abort();

      return res.status(413).send(
        "This file is larger than the current 512 MB download limit."
      );
    }

    const sourceType =
      upstream.headers.get("content-type") || "";

    if (
      sourceType.includes("text/html") ||
      sourceType.includes("application/json")
    ) {
      controller.abort();

      return res.status(502).send(
        "The provider returned a webpage instead of a media file."
      );
    }

    const extension =
      String(job.format || "").toLowerCase() === "mp3"
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

    const timeout = setTimeout(() => {
      controller.abort();
    }, 10 * 60 * 1000);

    try {
      await pipeline(
        Readable.fromWeb(upstream.body),
        byteLimit,
        res,
        {
          signal: controller.signal
        }
      );

    } finally {
      clearTimeout(timeout);
    }

    console.log(
      `[Download] ${jobId} streamed ${bytesSent} bytes to client`
    );

  } catch (error) {
    if (!res.headersSent) {
      console.error("[Download stream error]", error.message);

      return res.status(502).send(
        "The file could not be streamed. Please try again."
      );
    }

    console.error(
      "[Download stream interrupted]",
      error.message
    );

    res.destroy(error);
  }
});

// UNKNOWN API ROUTE
app.use("/api", (_req, res) => {
  res.status(404).json({
    success: false,
    message: "API endpoint not found."
  });
});

// ERROR HANDLER
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

// START SERVER
app.listen(PORT, () => {
  console.log(
    `Vybemuse server listening on port ${PORT}`
  );

  console.log(
    `VidFetch API key: ${
      VIDFETCH_API_KEY ? "configured" : "missing"
    }`
  );

  console.log(
    `VidFetch information timeout: ${
      INFO_TIMEOUT_MS / 60000
    } minutes`
  );
});
