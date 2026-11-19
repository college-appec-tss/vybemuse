
require("dotenv").config();

const express = require("express");
const helmet = require("helmet");
const rateLimit = require("express-rate-limit");
const path = require("path");

const app = express();
const PORT = process.env.PORT || 3000;

const VIDFETCH_API = "https://api.vidfetch.dev";
const VIDFETCH_KEY = process.env.VIDFETCH_API_KEY;

app.use(helmet({
  contentSecurityPolicy: false
}));

app.use(express.json({ limit: "10kb" }));

app.use("/api", rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 40,
  standardHeaders: true,
  legacyHeaders: false
}));

// Serve index.html directly from the root folder.
app.use(express.static(__dirname));

// Supported public video sources.
const allowedHosts = [
  "youtube.com",
  "www.youtube.com",
  "youtu.be",
  "m.youtube.com",
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

function validateVideoUrl(value) {
  if (typeof value !== "string" || value.length > 2048) {
    return null;
  }

  try {
    const url = new URL(value);

    if (url.protocol !== "https:") return null;

    const host = url.hostname.toLowerCase();

    if (!allowedHosts.includes(host)) return null;

    return url.href;
  } catch {
    return null;
  }
}

// VidFetch API helper.
async function vidfetch(endpoint, options = {}) {
  if (!VIDFETCH_KEY) {
    throw new Error("VidFetch API key is not configured.");
  }

  const response = await fetch(
    `${VIDFETCH_API}${endpoint}`,
    {
      ...options,
      signal: AbortSignal.timeout(30000),
      headers: {
        Authorization: `Bearer ${VIDFETCH_KEY}`,
        "Content-Type": "application/json",
        ...options.headers
      }
    }
  );

  const data = await response.json().catch(() => ({}));

  if (!response.ok) {
    const error = new Error(
      data.message || data.error || "VidFetch request failed."
    );

    error.status = response.status;
    throw error;
  }

  return data;
}

// Homepage.
app.get("/", (req, res) => {
  res.sendFile(path.join(__dirname, "index.html"));
});

// Server status.
app.get("/api/health", (req, res) => {
  res.json({
    success: true,
    service: "Vybemuse",
    status: "online"
  });
});

// Check video information before downloading.
app.post("/api/video/info", async (req, res) => {
  try {
    const videoUrl = validateVideoUrl(req.body.url);

    if (!videoUrl) {
      return res.status(400).json({
        success: false,
        message: "Unsupported or invalid video URL."
      });
    }

    const data = await vidfetch("/v1/info", {
      method: "POST",
      body: JSON.stringify({
        url: videoUrl
      })
    });

    res.json({
      success: true,
      video: data
    });

  } catch (error) {
    console.error("Video information error:", error.message);

    res.status(error.status || 502).json({
      success: false,
      message: error.message
    });
  }
});

// Create a video download job.
app.post("/api/video/download", async (req, res) => {
  try {
    const videoUrl = validateVideoUrl(req.body.url);

    const allowedFormats = [
      "144p",
      "240p",
      "360p",
      "480p",
      "720p",
      "1080p",
      "1440p",
      "2160p",
      "mp3"
    ];

    const format = req.body.format || "720p";

    if (!videoUrl) {
      return res.status(400).json({
        success: false,
        message: "Please provide a supported video URL."
      });
    }

    if (!allowedFormats.includes(format)) {
      return res.status(400).json({
        success: false,
        message: "Unsupported video format."
      });
    }

    // Users must have permission to save the content.
    if (req.body.authorized !== true) {
      return res.status(403).json({
        success: false,
        message: "Please confirm that you own or have permission to download this content."
      });
    }

    const data = await vidfetch("/v1/downloads", {
      method: "POST",
      body: JSON.stringify({
        url: videoUrl,
        format
      })
    });

    res.status(202).json({
      success: true,
      message: "Download job created.",
      job: data
    });

  } catch (error) {
    console.error("Download creation error:", error.message);

    res.status(error.status || 502).json({
      success: false,
      message: error.message
    });
  }
});

// Check download progress.
app.get("/api/video/status/:id", async (req, res) => {
  try {
    const jobId = req.params.id;

    if (!/^dl_[a-zA-Z0-9_-]+$/.test(jobId)) {
      return res.status(400).json({
        success: false,
        message: "Invalid download job ID."
      });
    }

    const data = await vidfetch(
      `/v1/downloads/${encodeURIComponent(jobId)}`
    );

    res.json({
      success: true,
      job: data
    });

  } catch (error) {
    console.error("Download status error:", error.message);

    res.status(error.status || 502).json({
      success: false,
      message: error.message
    });
  }
});

// API errors.
app.use("/api", (req, res) => {
  res.status(404).json({
    success: false,
    message: "API endpoint not found."
  });
});

// Start application.
app.listen(PORT, () => {
  console.log("--------------------------------");
  console.log("       VYBEMUSE SERVER");
  console.log("--------------------------------");
  console.log(`Server: http://localhost:${PORT}`);
  console.log("Video API: Connected when configured");
});
