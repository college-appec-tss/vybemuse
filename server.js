
require("dotenv").config();

const express = require("express");
const path = require("path");
const { Readable } = require("node:stream");
const { pipeline } = require("node:stream/promises");

const app = express();
const PORT = process.env.PORT || 3000;

const API_KEY = process.env.VIDFETCH_API_KEY;
const API_BASE = "https://api.vidfetch.dev";

app.disable("x-powered-by");
app.use(express.json({ limit: "10kb" }));

// Basic API request limiting.
const requestCounts = new Map();

app.use("/api", (req, res, next) => {
  const ip = req.ip || req.socket.remoteAddress || "unknown";
  const now = Date.now();
  const current = requestCounts.get(ip);

  if (!current || now - current.start > 60 * 1000) {
    requestCounts.set(ip, { start: now, count: 1 });
    return next();
  }

  current.count++;

  if (current.count > 20) {
    return res.status(429).json({
      success: false,
      message: "Too many requests. Please wait a minute."
    });
  }

  next();
});

const SUPPORTED_HOSTS = [
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
];

const JOB_ID_PATTERN = /^dl_[a-zA-Z0-9_-]{4,100}$/;

function validateVideoUrl(value) {
  if (typeof value !== "string" || value.length > 2048) {
    return null;
  }

  try {
    const url = new URL(value.trim());

    if (url.protocol !== "https:") return null;
    if (url.username || url.password || url.port) return null;

    const hostname = url.hostname.toLowerCase();

    if (!SUPPORTED_HOSTS.includes(hostname)) return null;

    return url.href;
  } catch {
    return null;
  }
}

function validJobId(id) {
  return typeof id === "string" && JOB_ID_PATTERN.test(id);
}

async function vidfetch(endpoint, options = {}) {
  if (!API_KEY) {
    const error = new Error("VidFetch API key is not configured.");
    error.status = 503;
    throw error;
  }

  const response = await fetch(`${API_BASE}${endpoint}`, {
    method: options.method || "GET",
    headers: {
      Authorization: `Bearer ${API_KEY}`,
      Accept: "application/json",
      ...(options.body ? { "Content-Type": "application/json" } : {})
    },
    ...(options.body ? { body: JSON.stringify(options.body) } : {}),
    redirect: "error",
    signal: AbortSignal.timeout(options.timeout || 45000)
  });

  const raw = await response.text();

  let data;

  try {
    data = raw ? JSON.parse(raw) : {};
  } catch {
    throw new Error("VidFetch returned an invalid response.");
  }

  if (!response.ok) {
    const error = new Error(
      data.message ||
      data.error ||
      `VidFetch request failed (${response.status}).`
    );

    error.status = response.status === 429 ? 429 : 502;
    throw error;
  }

  return data;
}

function apiError(res, error) {
  console.error("[VidFetch]", error.message);

  return res.status(error.status || 502).json({
    success: false,
    message: error.message || "VidFetch service error."
  });
}

/* =====================================
   1. FETCH VIDEO INFORMATION
===================================== */

app.post("/api/video/info", async (req, res) => {
  const url = validateVideoUrl(req.body?.url);

  if (!url) {
    return res.status(400).json({
      success: false,
      message: "Enter a valid supported HTTPS video URL."
    });
  }

  try {
    const response = await vidfetch("/v1/info", {
      method: "POST",
      body: { url },
      timeout: 60000
    });

    const video = response.video || response;

    const formats = Array.isArray(video.formats)
      ? video.formats.map((item) => {
          if (typeof item === "string") {
            return {
              value: item,
              label: item.toUpperCase()
            };
          }

          const label = String(
            item.label ||
            item.format ||
            item.quality ||
            item.id ||
            ""
          );

          let value = String(
            item.format ||
            item.quality ||
            item.label ||
            item.id ||
            ""
          );

          if (/^\d+$/.test(value)) {
            value += "p";
          }

          return {
            value,
            label: label || value,
            type: item.type || "",
            size: item.filesize_str || item.size || null
          };
        }).filter((item) => item.value)
      : [];

    return res.json({
      success: true,
      video: {
        title: video.title || video.name || "Untitled video",
        thumbnail: video.thumbnail || video.thumbnail_url || null,
        duration: video.duration ?? null,
        uploader: video.uploader || video.channel || "",
        source: video.source || "",
        formats
      }
    });
  } catch (error) {
    return apiError(res, error);
  }
});

/* =====================================
   2. CREATE DOWNLOAD JOB
===================================== */

app.post("/api/video/download", async (req, res) => {
  const url = validateVideoUrl(req.body?.url);
  const format = String(req.body?.format || "").trim();

  if (!url) {
    return res.status(400).json({
      success: false,
      message: "Invalid video URL."
    });
  }

  if (!format || !/^(144p|240p|360p|480p|720p|1080p|1440p|2160p|mp3)$/i.test(format)) {
    return res.status(400).json({
      success: false,
      message: "Choose a supported format."
    });
  }

  if (req.body?.authorized !== true) {
    return res.status(403).json({
      success: false,
      message: "Please confirm that you own or have permission to download this media."
    });
  }

  try {
    const response = await vidfetch("/v1/downloads", {
      method: "POST",
      body: {
        url,
        format
      },
      timeout: 60000
    });

    if (!validJobId(response.id)) {
      throw new Error("VidFetch did not return a valid download job ID.");
    }

    return res.status(202).json({
      success: true,
      job: {
        id: response.id,
        status: response.status || "queued",
        format: response.format || format
      }
    });
  } catch (error) {
    return apiError(res, error);
  }
});

/* =====================================
   3. CHECK DOWNLOAD STATUS
===================================== */

app.get("/api/video/status/:id", async (req, res) => {
  const id = req.params.id;

  if (!validJobId(id)) {
    return res.status(400).json({
      success: false,
      message: "Invalid download ID."
    });
  }

  try {
    const response = await vidfetch(
      `/v1/downloads/${encodeURIComponent(id)}`,
      { timeout: 30000 }
    );

    const status = String(response.status || "unknown").toLowerCase();

    return res.json({
      success: true,
      job: {
        id: response.id || id,
        status,
        format: response.format || "",
        message: response.message || response.error_message || "",
        downloadReady: status === "completed" &&
          typeof response.download_url === "string",
        previewUrl: status === "completed" &&
          typeof response.download_url === "string"
          ? response.download_url
          : null
      }
    });
  } catch (error) {
    return apiError(res, error);
  }
});

/* =====================================
   4. STREAM FILE THROUGH VYBEMUSE
===================================== */

app.get("/api/video/file/:id", async (req, res) => {
  const id = req.params.id;

  if (!validJobId(id)) {
    return res.status(400).json({
      success: false,
      message: "Invalid download ID."
    });
  }

  try {
    // Get the signed URL directly from VidFetch.
    const job = await vidfetch(
      `/v1/downloads/${encodeURIComponent(id)}`,
      { timeout: 30000 }
    );

    if (
      String(job.status).toLowerCase() !== "completed" ||
      !job.download_url
    ) {
      return res.status(409).json({
        success: false,
        message: "Your file is not ready yet."
      });
    }

    let downloadUrl;

    try {
      downloadUrl = new URL(job.download_url);
    } catch {
      throw new Error("VidFetch returned an invalid download URL.");
    }

    if (
      downloadUrl.protocol !== "https:" ||
      downloadUrl.username ||
      downloadUrl.password ||
      downloadUrl.port
    ) {
      throw new Error("Unsupported download URL.");
    }

    // This URL comes from the authenticated VidFetch API,
    // not from a URL supplied by the website visitor.
    const upstream = await fetch(downloadUrl, {
      redirect: "error",
      signal: AbortSignal.timeout(10 * 60 * 1000)
    });

    if (!upstream.ok || !upstream.body) {
      throw new Error("Unable to retrieve the prepared file.");
    }

    const isMp3 = String(job.format || "").toLowerCase() === "mp3";
    const extension = isMp3 ? "mp3" : "mp4";
    const filename = `vybemuse-${id}.${extension}`;

    res.status(upstream.status);
    res.setHeader(
      "Content-Type",
      isMp3 ? "audio/mpeg" :
      upstream.headers.get("content-type") || "video/mp4"
    );
    res.setHeader(
      "Content-Disposition",
      `attachment; filename="${filename}"`
    );
    res.setHeader("Cache-Control", "private, no-store");
    res.setHeader("X-Content-Type-Options", "nosniff");

    const contentLength = upstream.headers.get("content-length");

    if (contentLength) {
      res.setHeader("Content-Length", contentLength);
    }

    await pipeline(Readable.fromWeb(upstream.body), res);
  } catch (error) {
    console.error("[File download]", error.message);

    if (!res.headersSent) {
      return res.status(502).json({
        success: false,
        message: "Could not download the file. Please try again."
      });
    }

    res.destroy(error);
  }
});

/* =====================================
   5. HEALTH CHECK
===================================== */

app.get("/api/health", (_req, res) => {
  res.json({
    success: true,
    service: "Vybemuse",
    status: "online",
    vidfetchConfigured: Boolean(API_KEY)
  });
});

/* =====================================
   6. WEBSITE
===================================== */

app.use(express.static(__dirname, {
  index: false,
  dotfiles: "ignore"
}));

app.get("/", (_req, res) => {
  res.sendFile(path.join(__dirname, "index.html"));
});

app.use("/api", (_req, res) => {
  res.status(404).json({
    success: false,
    message: "API endpoint not found."
  });
});

app.listen(PORT, () => {
  console.log(`Vybemuse running on port ${PORT}`);
  console.log(`VidFetch API: ${API_KEY ? "configured" : "missing API key"}`);
});
