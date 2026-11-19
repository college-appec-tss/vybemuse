require("dotenv").config();

const express = require("express");
const helmet = require("helmet");
const rateLimit = require("express-rate-limit");
const path = require("path");

const app = express();

const PORT = Number(process.env.PORT) || 3000;
const VIDFETCH_API = "https://api.vidfetch.dev";
const VIDFETCH_API_KEY = process.env.VIDFETCH_API_KEY;
const SITE_URL = (
  process.env.SITE_URL || "https://vybemuse.onrender.com"
).replace(/\/+$/, "");

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
        connectSrc: ["'self'"],
        upgradeInsecureRequests: []
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
    legacyHeaders: false,
    message: {
      success: false,
      message: "Too many requests. Please wait a little and try again."
    }
  })
);

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
  const safe = typeof message === "string" ? message.trim() : "";

  return safe && safe.length < 300
    ? safe
    : fallback;
}

async function vidfetch(
  endpoint,
  {
    method = "GET",
    body,
    timeoutMs = 45000
  } = {}
) {
  if (!VIDFETCH_API_KEY) {
    const error = new Error(
      "The media service is not configured yet. Please try again later."
    );

    error.status = 503;
    throw error;
  }

  const controller = new AbortController();
  const timeout = setTimeout(
    () => controller.abort(),
    timeoutMs
  );

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
      const message = publicErrorMessage(
        data.message || data.error || data.detail,
        `Media provider returned HTTP ${response.status}.`
      );

      const error = new Error(message);

      error.status = response.status === 429 ? 429 : 502;
      throw error;
    }

    console.log(
      `[VidFetch] ${method} ${endpoint} -> ${response.status} in ${Date.now() - startedAt}ms`
    );

    return data;
  } catch (error) {
    if (error.name === "AbortError") {
      console.error(
        `[VidFetch] timeout: ${method} ${endpoint} after ${Date.now() - startedAt}ms`
      );

      const timeoutError = new Error(
        "The media provider took too long to respond. Please retry."
      );

      timeoutError.status = 504;
      throw timeoutError;
    }

    if (error.name === "TypeError") {
      console.error(
        `[VidFetch] network error: ${method} ${endpoint}: ${error.message}`
      );

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

/* Serve the website */

app.get("/", (_req, res) => {
  res.sendFile(path.join(__dirname, "index.html"));
});

app.use(
  express.static(__dirname, {
    maxAge: "7d",
    index: false,
    setHeaders(res, filePath) {
      if (filePath.endsWith(".html")) {
        res.setHeader("Cache-Control", "no-cache");
      }
    }
  })
);

/* SEO files */

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

/* Health check */

app.get("/api/health", (_req, res) => {
  res.json({
    success: true,
    service: "Vybemuse",
    status: "online",
    mediaProviderConfigured: Boolean(VIDFETCH_API_KEY)
  });
});

/* Get media information */

app.post("/api/video/info", async (req, res) => {
  const url = validateVideoUrl(req.body?.url);

  if (!url) {
    return res.status(400).json({
      success: false,
      message: "Enter a valid HTTPS video URL from a supported platform."
    });
  }

  try {
    const data = await vidfetch("/v1/info", {
      method: "POST",
      body: { url },
      timeoutMs: 45000
    });

    return res.json({
      success: true,
      video: data
    });
  } catch (error) {
    return sendApiError(
      res,
      error,
      "Could not retrieve video information."
    );
  }
});

/* Create download job */

app.post("/api/video/download", async (req, res) => {
  const url = validateVideoUrl(req.body?.url);

  const format =
    typeof req.body?.format === "string"
      ? req.body.format
      : "720p";

  if (!url) {
    return res.status(400).json({
      success: false,
      message: "Enter a valid HTTPS video URL from a supported platform."
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
        format
      },
      timeoutMs: 45000
    });

    if (
      !data ||
      typeof data.id !== "string" ||
      !/^dl_[a-zA-Z0-9_-]{4,100}$/.test(data.id)
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
      message: "Download job created.",
      job: {
        id: data.id,
        status: data.status || "queued",
        format: data.format || format
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

/* Check download job status */

app.get("/api/video/status/:id", async (req, res) => {
  const jobId = req.params.id;

  if (!/^dl_[a-zA-Z0-9_-]{4,100}$/.test(jobId)) {
    return res.status(400).json({
      success: false,
      message: "Invalid download job ID."
    });
  }

  try {
    const data = await vidfetch(
      `/v1/downloads/${encodeURIComponent(jobId)}`,
      {
        method: "GET",
        timeoutMs: 30000
      }
    );

    const status = String(
      data.status || "unknown"
    ).toLowerCase();

    const downloadUrl =
      typeof data.download_url === "string"
        ? data.download_url
        : null;

    return res.json({
      success: true,
      job: {
        id: data.id || jobId,
        status,
        format: data.format || null,
        downloadUrl,
        message: data.message || data.error || null
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

/* Unknown API endpoint */

app.use("/api", (_req, res) => {
  res.status(404).json({
    success: false,
    message: "API endpoint not found."
  });
});

/* General error handler */

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

/* Start server */

app.listen(PORT, () => {
  console.log(`Vybemuse server listening on port ${PORT}`);

  console.log(
    `VidFetch API key: ${VIDFETCH_API_KEY ? "configured" : "missing"}`
  );
});
