require("dotenv").config();

const express = require("express");
const helmet = require("helmet");
const rateLimit = require("express-rate-limit");
const path = require("path");

const app = express();
const PORT = process.env.PORT || 3000;

const VIDFETCH_API = "https://api.vidfetch.dev";
const VIDFETCH_KEY = process.env.VIDFETCH_API_KEY;

// Security headers
app.use(
  helmet({
    contentSecurityPolicy: false
  })
);

// Request parsing
app.use(express.json({
  limit: "10kb"
}));

// API rate limiting
app.use(
  "/api",
  rateLimit({
    windowMs: 15 * 60 * 1000,
    max: 40,
    standardHeaders: true,
    legacyHeaders: false
  })
);

// Supported video hosts
const allowedHosts = new Set([
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
]);

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

// Validate video URL
function validateVideoUrl(value) {
  if (
    typeof value !== "string" ||
    value.length > 2048
  ) {
    return null;
  }

  try {
    const url = new URL(value);

    if (url.protocol !== "https:") return null;
    if (!allowedHosts.has(url.hostname.toLowerCase())) return null;
    if (url.port || url.username || url.password) return null;

    return url.href;
  } catch {
    return null;
  }
}

// VidFetch API helper
async function vidfetch(endpoint, options = {}) {
  if (!VIDFETCH_KEY) {
    const error = new Error(
      "VIDFETCH_API_KEY is missing from environment variables."
    );

    error.status = 503;
    throw error;
  }

  const controller = new AbortController();

  const timeout = setTimeout(() => {
    controller.abort();
  }, 30000);

  try {
    const response = await fetch(
      `${VIDFETCH_API}${endpoint}`,
      {
        ...options,
        redirect: "error",
        signal: controller.signal,
        headers: {
          Authorization: `Bearer ${VIDFETCH_KEY}`,
          "Content-Type": "application/json",
          Accept: "application/json",
          ...options.headers
        }
      }
    );

    const responseText = await response.text();

    let data;

    try {
      data = JSON.parse(responseText);
    } catch {
      const error = new Error(
        `VidFetch returned an invalid response. HTTP ${response.status}`
      );

      error.status = 502;
      throw error;
    }

    if (!response.ok) {
      const error = new Error(
        data.message ||
        data.error ||
        `VidFetch HTTP error: ${response.status}`
      );

      error.status = response.status === 429 ? 429 : 502;
      throw error;
    }

    return data;

  } catch (error) {
    if (error.name === "AbortError") {
      const timeoutError = new Error(
        "VidFetch request timed out."
      );

      timeoutError.status = 504;
      throw timeoutError;
    }

    if (error.name === "TypeError") {
      const networkError = new Error(
        "Unable to connect to VidFetch. Please try again."
      );

      networkError.status = 502;
      throw networkError;
    }

    throw error;

  } finally {
    clearTimeout(timeout);
  }
}

// Homepage
app.get("/", (req, res) => {
  res.sendFile(path.join(__dirname, "index.html"));
});

// Health check
app.get("/api/health", (req, res) => {
  res.json({
    success: true,
    service: "Vybemuse",
    status: "online"
  });
});

// Retrieve video information
app.post("/api/video/info", async (req, res) => {
  try {
    const videoUrl = validateVideoUrl(req.body?.url);

    if (!videoUrl) {
      return res.status(400).json({
        success: false,
        message: "Unsupported or invalid HTTPS video URL."
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
    console.error(
      "Video information error:",
      error.message
    );

    res.status(error.status || 502).json({
      success: false,
      message: error.message
    });
  }
});

// Create download job
app.post("/api/video/download", async (req, res) => {
  try {
    const videoUrl = validateVideoUrl(req.body?.url);

    const format = req.body?.format || "720p";

    if (!videoUrl) {
      return res.status(400).json({
        success: false,
        message: "Please provide a supported video URL."
      });
    }

    if (!allowedFormats.has(format)) {
      return res.status(400).json({
        success: false,
        message: "Unsupported video format."
      });
    }

    // Require explicit user confirmation.
    if (req.body?.authorized !== true) {
      return res.status(403).json({
        success: false,
        message: "Please confirm you have permission to download this content."
      });
    }

    const data = await vidfetch("/v1/downloads", {
      method: "POST",
      body: JSON.stringify({
        url: videoUrl,
        format
      })
    });

    // VidFetch documentation specifies an id.
    if (
      !data ||
      typeof data.id !== "string" ||
      !/^dl_[a-zA-Z0-9_-]+$/.test(data.id)
    ) {
      return res.status(502).json({
        success: false,
        message: "VidFetch did not return a valid download job ID."
      });
    }

    res.status(202).json({
      success: true,
      message: "Download job created.",
      job: data
    });

  } catch (error) {
    console.error(
      "Download creation error:",
      error.message
    );

    res.status(error.status || 502).json({
      success: false,
      message: error.message
    });
  }
});

// Check download progress
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
    console.error(
      "Download status error:",
      error.message
    );

    res.status(error.status || 502).json({
      success: false,
      message: error.message
    });
  }
});

// API catch-all MUST remain last
app.use("/api", (req, res) => {
  res.status(404).json({
    success: false,
    message: "API endpoint not found."
  });
});

// Handle malformed JSON and other request errors
app.use((error, req, res, next) => {
  if (res.headersSent) {
    return next(error);
  }

  if (error instanceof SyntaxError && error.status === 400) {
    return res.status(400).json({
      success: false,
      message: "Invalid JSON request."
    });
  }

  console.error("Server error:", error.message);

  res.status(500).json({
    success: false,
    message: "An unexpected server error occurred."
  });
});

// Start server
app.listen(PORT, () => {
  console.log("--------------------------------");
  console.log("       VYBEMUSE SERVER");
  console.log("--------------------------------");
  console.log(`Server running on port ${PORT}`);
  console.log(
    "VidFetch key:",
    VIDFETCH_KEY ? "Configured" : "Missing"
  );
});
