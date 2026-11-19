
require("dotenv").config();

const express = require("express");
const helmet = require("helmet");
const rateLimit = require("express-rate-limit");
const path = require("path");
const { Readable } = require("node:stream");
const { pipeline } = require("node:stream/promises");
const net = require("node:net");

const app = express();

const PORT = Number(process.env.PORT) || 3000;
const API_BASE = "https://api.vidfetch.dev";
const API_KEY = process.env.VIDFETCH_API_KEY;

const SITE_URL = (
  process.env.SITE_URL || "https://vybemuse.onrender.com"
).replace(/\/+$/, "");

const DOWNLOAD_TIMEOUT = 10 * 60 * 1000;
const JOB_ID_PATTERN = /^dl_[a-zA-Z0-9_-]{4,100}$/;

const ALLOWED_HOSTS = new Set([
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

app.disable("x-powered-by");
app.set("trust proxy", 1);

app.use(
  helmet({
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
        connectSrc: ["'self'"]
      }
    },
    crossOriginEmbedderPolicy: false
  })
);

app.use(express.json({ limit: "12kb" }));

app.use(
  "/api",
  rateLimit({
    windowMs: 15 * 60 * 1000,
    limit: 40,
    standardHeaders: "draft-8",
    legacyHeaders: false
  })
);

/* -----------------------------
   Utility functions
----------------------------- */

function validateVideoUrl(value) {
  if (typeof value !== "string" || value.length > 2048) {
    return null;
  }

  try {
    const url = new URL(value.trim());

    if (url.protocol !== "https:") return null;
    if (!ALLOWED_HOSTS.has(url.hostname.toLowerCase())) return null;
    if (url.username || url.password || url.port) return null;

    return url.href;
  } catch {
    return null;
  }
}

function validJobId(id) {
  return typeof id === "string" && JOB_ID_PATTERN.test(id);
}

function apiError(res, error, fallback) {
  console.error("[Vybemuse API]", error.message);

  const status = Number(error.status) || 502;

  return res.status(status).json({
    success: false,
    message: error.message || fallback
  });
}

async function readProviderResponse(response) {
  const text = await response.text();

  let data = {};

  if (text) {
    try {
      data = JSON.parse(text);
    } catch {
      throw new Error("VidFetch returned an invalid response.");
    }
  }

  if (!response.ok) {
    const error = new Error(
      data.error_message ||
      data.message ||
      data.error ||
      `VidFetch returned HTTP ${response.status}.`
    );

    error.status = response.status === 429 ? 429 : 502;
    throw error;
  }

  return data;
}

async function vidfetch(endpoint, options = {}) {
  if (!API_KEY) {
    const error = new Error("The media service is not configured.");
    error.status = 503;
    throw error;
  }

  const controller = new AbortController();

  const timeout = setTimeout(
    () => controller.abort(),
    options.timeout || 45000
  );

  try {
    const response = await fetch(`${API_BASE}${endpoint}`, {
      method: options.method || "GET",
      redirect: "error",
      signal: controller.signal,
      headers: {
        Authorization: `Bearer ${API_KEY}`,
        Accept: "application/json",
        ...(options.body
          ? { "Content-Type": "application/json" }
          : {})
      },
      ...(options.body
        ? { body: JSON.stringify(options.body) }
        : {})
    });

    return await readProviderResponse(response);
  } catch (error) {
    if (error.name === "AbortError") {
      const timeoutError = new Error(
        "VidFetch took too long to respond. Please try again."
      );

      timeoutError.status = 504;
      throw timeoutError;
    }

    throw error;
  } finally {
    clearTimeout(timeout);
  }
}

/* -----------------------------
   Download URL validation
----------------------------- */

function isPrivateIPv4(ip) {
  const parts = ip.split(".").map(Number);

  if (
    parts.length !== 4 ||
    parts.some((part) => !Number.isInteger(part) || part < 0 || part > 255)
  ) {
    return true;
  }

  const [a, b] = parts;

  return (
    a === 0 ||
    a === 10 ||
    a === 127 ||
    a >= 224 ||
    (a === 169 && b === 254) ||
    (a === 172 && b >= 16 && b <= 31) ||
    (a === 192 && b === 168) ||
    (a === 100 && b >= 64 && b <= 127) ||
    (a === 198 && (b === 18 || b === 19))
  );
}

function isPrivateIPv6(ip) {
  const value = ip.toLowerCase();

  return (
    value === "::" ||
    value === "::1" ||
    value.startsWith("fc") ||
    value.startsWith("fd") ||
    value.startsWith("fe8") ||
    value.startsWith("fe9") ||
    value.startsWith("fea") ||
    value.startsWith("feb")
  );
}

function validateDownloadUrl(value) {
  if (typeof value !== "string" || value.length > 4096) {
    throw new Error("Invalid download URL.");
  }

  let url;

  try {
    url = new URL(value);
  } catch {
    throw new Error("The provider returned an invalid download URL.");
  }

  if (url.protocol !== "https:") {
    throw new Error("The provider returned an insecure download URL.");
  }

  if (url.username || url.password || url.port) {
    throw new Error("The provider returned an unsupported download URL.");
  }

  const hostname = url.hostname.toLowerCase();
  const ipVersion = net.isIP(hostname);

  if (ipVersion === 4 && isPrivateIPv4(hostname)) {
    throw new Error("The provider returned an invalid download host.");
  }

  if (ipVersion === 6 && isPrivateIPv6(hostname)) {
    throw new Error("The provider returned an invalid download host.");
  }

  if (
    hostname === "localhost" ||
    hostname.endsWith(".localhost") ||
    hostname.endsWith(".local") ||
    hostname.endsWith(".internal")
  ) {
    throw new Error("The provider returned an invalid download host.");
  }

  return url;
}

/* -----------------------------
   Website files
----------------------------- */

app.get("/", (_req, res) => {
  res.sendFile(path.join(__dirname, "index.html"));
});

app.use(
  express.static(__dirname, {
    index: false,
    maxAge: "7d",
    setHeaders(res, filePath) {
      if (filePath.endsWith(".html")) {
        res.setHeader("Cache-Control", "no-cache");
      }
    }
  })
);

/* -----------------------------
   SEO
----------------------------- */

app.get("/robots.txt", (_req, res) => {
  res.type("text/plain").send(
    `User-agent: *
Allow: /
Disallow: /api/
Sitemap: ${SITE_URL}/sitemap.xml
`
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

/* -----------------------------
   Health check
----------------------------- */

app.get("/api/health", (_req, res) => {
  res.json({
    success: true,
    service: "Vybemuse",
    status: "online",
    providerConfigured: Boolean(API_KEY)
  });
});

/* -----------------------------
   Retrieve video information
----------------------------- */

app.post("/api/video/info", async (req, res) => {
  const url = validateVideoUrl(req.body?.url);

  if (!url) {
    return res.status(400).json({
      success: false,
      message: "Enter a valid supported HTTPS video URL."
    });
  }

  try {
    const data = await vidfetch("/v1/info", {
      method: "POST",
      body: { url },
      timeout: 60000
    });

    return res.json({
      success: true,
      video: data.video || data
    });
  } catch (error) {
    return apiError(
      res,
      error,
      "Could not retrieve video information."
    );
  }
});

/* -----------------------------
   Create download job
----------------------------- */

app.post("/api/video/download", async (req, res) => {
  const url = validateVideoUrl(req.body?.url);
  const format = req.body?.format;

  if (!url) {
    return res.status(400).json({
      success: false,
      message: "Enter a valid supported HTTPS video URL."
    });
  }

  if (!ALLOWED_FORMATS.has(format)) {
    return res.status(400).json({
      success: false,
      message: "Select a supported video format."
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
        format
      },
      timeout: 60000
    });

    if (!validJobId(data.id)) {
      console.error(
        "[VidFetch] Unexpected job response:",
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
        id: data.id,
        status: data.status || "queued",
        format: data.format || format
      }
    });
  } catch (error) {
    return apiError(
      res,
      error,
      "Could not create the download job."
    );
  }
});

/* -----------------------------
   Check job status
----------------------------- */

app.get("/api/video/status/:id", async (req, res) => {
  const id = req.params.id;

  if (!validJobId(id)) {
    return res.status(400).json({
      success: false,
      message: "Invalid download job ID."
    });
  }

  try {
    const data = await vidfetch(
      `/v1/downloads/${encodeURIComponent(id)}`,
      { timeout: 30000 }
    );

    const status = String(data.status || "unknown").toLowerCase();

    return res.json({
      success: true,
      job: {
        id: data.id || id,
        status,
        format: data.format || null,
        message: data.error_message || data.message || null,

        // The signed URL stays on the server.
        // The browser uses our own download endpoint.
        downloadReady: Boolean(
          status === "completed" && data.download_url
        )
      }
    });
  } catch (error) {
    return apiError(
      res,
      error,
      "Could not check download progress."
    );
  }
});

/* -----------------------------
   Stream file through Vybemuse
----------------------------- */

app.get("/api/video/file/:id", async (req, res) => {
  const id = req.params.id;

  if (!validJobId(id)) {
    return res.status(400).json({
      success: false,
      message: "Invalid download job ID."
    });
  }

  let upstream;

  const controller = new AbortController();

  const timeout = setTimeout(
    () => controller.abort(),
    DOWNLOAD_TIMEOUT
  );

  try {
    // First, check whether VidFetch has finished processing.
    const job = await vidfetch(
      `/v1/downloads/${encodeURIComponent(id)}`,
      { timeout: 30000 }
    );

    const status = String(job.status || "").toLowerCase();

    if (["failed", "error", "cancelled", "canceled"].includes(status)) {
      return res.status(422).json({
        success: false,
        message: job.error_message || job.message || "The download failed."
      });
    }

    if (status !== "completed" || !job.download_url) {
      return res.status(409).json({
        success: false,
        message: "Your file is still processing. Please try again shortly.",
        status: status || "processing"
      });
    }

    // Validate the signed URL returned by VidFetch.
    const downloadUrl = validateDownloadUrl(job.download_url);

    // Forward browser range requests when supported by the provider.
    const headers = {};

    if (req.headers.range) {
      headers.Range = req.headers.range;
    }

    // Fetch the file on the server.
    // Redirects are disabled to avoid forwarding requests to unknown hosts.
    upstream = await fetch(downloadUrl, {
      method: "GET",
      headers,
      redirect: "error",
      signal: controller.signal
    });

    if (!upstream.ok || !upstream.body) {
      throw new Error(
        `Could not retrieve the file from the provider (HTTP ${upstream.status}).`
      );
    }

    const contentType =
      upstream.headers.get("content-type") || "application/octet-stream";

    const contentLength = upstream.headers.get("content-length");
    const contentRange = upstream.headers.get("content-range");
    const acceptRanges = upstream.headers.get("accept-ranges");

    // Build a safe filename.
    const extension = job.format === "mp3" ? "mp3" : "mp4";
    const filename = `vybemuse-${id}.${extension}`;

    res.status(upstream.status);

    res.setHeader("Content-Type", contentType);
    res.setHeader(
      "Content-Disposition",
      `attachment; filename="${filename}"; filename*=UTF-8''${encodeURIComponent(filename)}`
    );

    res.setHeader("Cache-Control", "private, no-store");
    res.setHeader("X-Content-Type-Options", "nosniff");

    if (contentLength) {
      res.setHeader("Content-Length", contentLength);
    }

    if (contentRange) {
      res.setHeader("Content-Range", contentRange);
    }

    if (acceptRanges) {
      res.setHeader("Accept-Ranges", acceptRanges);
    }

    // Stream instead of loading the entire video into server memory.
    await pipeline(
      Readable.fromWeb(upstream.body),
      res
    );

  } catch (error) {
    if (error.name === "AbortError") {
      console.error("[Download] Streaming timeout.");

      if (!res.headersSent) {
        res.status(504).json({
          success: false,
          message: "The download took too long. Please try again."
        });
      } else {
        res.destroy(error);
      }

      return;
    }

    console.error("[Download streaming error]", error.message);

    if (!res.headersSent) {
      res.status(502).json({
        success: false,
        message: "Unable to stream this file. Please try again."
      });
    } else {
      res.destroy(error);
    }
  } finally {
    clearTimeout(timeout);
  }
});

/* -----------------------------
   Unknown API routes
----------------------------- */

app.use("/api", (_req, res) => {
  res.status(404).json({
    success: false,
    message: "API endpoint not found."
  });
});

/* -----------------------------
   General error handler
----------------------------- */

app.use((error, _req, res, _next) => {
  console.error("[Server error]", error.message);

  if (res.headersSent) {
    return res.destroy(error);
  }

  return res.status(500).json({
    success: false,
    message: "An unexpected server error occurred."
  });
});

/* -----------------------------
   Start server
----------------------------- */

app.listen(PORT, () => {
  console.log(`Vybemuse running on port ${PORT}`);

  console.log(
    `VidFetch API key: ${API_KEY ? "configured" : "missing"}`
  );
});
