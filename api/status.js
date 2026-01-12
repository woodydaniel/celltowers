/**
 * Vercel Serverless Function (Node.js) – lightweight proxy to the scraper status endpoint.
 *
 * Env vars (set in Vercel):
 *   - SCRAPER_STATUS_URL or CELLMAPPER_API_URL
 *   - SCRAPER_STATUS_BEARER_TOKEN or CELLMAPPER_API_TOKEN
 *   - ALLOW_INSECURE_STATUS_URL = "true" to allow http:// (default: false)
 */

function utcNowIso() {
  return new Date().toISOString();
}

const FINISH_BUFFER_DAYS = 6;
const FINISH_BUFFER_MS = FINISH_BUFFER_DAYS * 24 * 60 * 60 * 1000;

function applyExpectedTotals(normalized) {
  const expectedTotal = Number.parseInt(process.env.EXPECTED_TOTAL_TILES || "", 10);
  if (!Number.isFinite(expectedTotal) || expectedTotal <= 0) return normalized;
  if (!normalized || typeof normalized !== "object" || !normalized.progress) return normalized;

  const out = { ...normalized, progress: { ...normalized.progress } };
  const completed = Number.isFinite(out.progress.completed_tiles) ? Number(out.progress.completed_tiles) : null;

  out.progress.total_tiles = expectedTotal;
  if (completed != null) {
    out.progress.pending_tiles = Math.max(0, expectedTotal - Math.trunc(completed));
    out.progress.completion_percent = expectedTotal > 0 ? (Number(completed) / expectedTotal) * 100 : null;
  } else {
    out.progress.pending_tiles = null;
    out.progress.completion_percent = null;
  }
  return out;
}

function humanEta(hours) {
  if (hours == null || !Number.isFinite(hours) || hours < 0) return "—";
  if (hours < 1) return `~${Math.max(0, Math.floor(hours * 60))}m`;
  if (hours < 48) return `~${hours.toFixed(1)}h`;
  return `~${(hours / 24).toFixed(1)}d`;
}

function normalizeUpstream(data) {
  // If upstream already matches our dashboard schema, pass through (with stripping).
  if (data && typeof data === "object" && data.generated_at && data.progress && data.eta) {
    const out = { ...data };
    delete out.debug;
    out.workers = [];

    // Apply +6 day buffer to projected finish (and keep ETA consistent).
    if (out.eta && typeof out.eta === "object") {
      if (out.eta.projected_finish_at) {
        const d = new Date(out.eta.projected_finish_at);
        if (!Number.isNaN(d.getTime())) {
          out.eta.projected_finish_at = new Date(d.getTime() + FINISH_BUFFER_MS).toISOString();
        }
      }
      if (Number.isFinite(out.eta.eta_hours)) {
        out.eta.eta_hours = Number(out.eta.eta_hours) + FINISH_BUFFER_DAYS * 24;
        out.eta.eta_human = humanEta(out.eta.eta_hours);
      }
    }

    return applyExpectedTotals(out);
  }

  const ts = (data && (data.timestamp || data.generated_at)) ? String(data.timestamp || data.generated_at) : null;
  const status = data && data.status ? String(data.status).toLowerCase().trim() : "";
  const active = ["running", "live", "active"].includes(status);

  const prog = data && typeof data.progress === "object" ? data.progress : {};
  const totalTiles = prog.total_tiles;
  const completedTiles = prog.total_completed_tiles;
  let pct = prog.overall_progress_pct;
  if (pct == null && Number.isFinite(totalTiles) && totalTiles && Number.isFinite(completedTiles)) {
    pct = (completedTiles / totalTiles) * 100;
  }

  const towers = data && typeof data.towers === "object" ? data.towers : {};
  const towersTotal = towers.total;

  const est = data && typeof data.estimate === "object" ? data.estimate : {};
  const remainingHours = est.remaining_hours;

  const nowIso = utcNowIso();
  let projectedFinishAt = null;
  if (Number.isFinite(remainingHours) && remainingHours >= 0) {
    projectedFinishAt = new Date(Date.now() + remainingHours * 3600 * 1000 + FINISH_BUFFER_MS).toISOString();
  }

  let pendingTiles = null;
  if (Number.isFinite(totalTiles) && Number.isFinite(completedTiles)) {
    pendingTiles = Math.max(0, Math.trunc(totalTiles) - Math.trunc(completedTiles));
  }

  return {
    ok: true,
    active: Boolean(active),
    run_id: "live",
    generated_at: ts || nowIso,
    server_time: nowIso,
    progress: {
      total_tiles: Number.isFinite(totalTiles) ? Math.trunc(totalTiles) : null,
      completed_tiles: Number.isFinite(completedTiles) ? Math.trunc(completedTiles) : null,
      pending_tiles: pendingTiles,
      deferred_tiles: null,
      completion_percent: Number.isFinite(pct) ? Number(pct) : null,
      towers_collected: Number.isFinite(towersTotal) ? Math.trunc(towersTotal) : 0,
    },
    velocity: { tiles_per_hour: null },
    health: { state: active ? "good" : "idle" },
    eta: {
      eta_hours: Number.isFinite(remainingHours) ? Number(remainingHours) + FINISH_BUFFER_DAYS * 24 : null,
      eta_human: Number.isFinite(remainingHours) ? humanEta(Number(remainingHours) + FINISH_BUFFER_DAYS * 24) : "—",
      projected_finish_at: projectedFinishAt,
    },
    workers: [],
    debug: null,
  };
}

module.exports = async (req, res) => {
  let stage = "start";
  try {
    const rawUrl =
      (process.env.SCRAPER_STATUS_URL || process.env.CELLMAPPER_API_URL || "").trim();
    const token =
      (process.env.SCRAPER_STATUS_BEARER_TOKEN || process.env.CELLMAPPER_API_TOKEN || "").trim();
    const allowInsecure = (process.env.ALLOW_INSECURE_STATUS_URL || "false").toLowerCase() === "true";
    const debug = (process.env.DEBUG_PROXY || "").toLowerCase() === "true";

    // Guard: people often paste env vars without newlines, causing concatenation.
    // Example: "http://.../api/statusCELLMAPPER_API_TOKEN=..."
    let url = rawUrl;
    const cutMarkers = [
      "CELLMAPPER_API_TOKEN=",
      "SCRAPER_STATUS_BEARER_TOKEN=",
      "SCRAPER_STATUS_URL=",
      "ALLOW_INSECURE_STATUS_URL=",
    ];
    for (const m of cutMarkers) {
      const idx = url.indexOf(m);
      if (idx > 0) {
        url = url.slice(0, idx).trim();
      }
    }

    if (!url) {
      res.status(500).json({ ok: false, error: "missing_env", env: "SCRAPER_STATUS_URL|CELLMAPPER_API_URL" });
      return;
    }
    if (!token) {
      res.status(500).json({ ok: false, error: "missing_env", env: "SCRAPER_STATUS_BEARER_TOKEN|CELLMAPPER_API_TOKEN" });
      return;
    }

    stage = "parse_url";
    let u;
    try {
      u = new URL(url);
    } catch {
      res.status(400).json({ ok: false, error: "invalid_url" });
      return;
    }
    if (u.protocol !== "https:" && u.protocol !== "http:") {
      res.status(400).json({ ok: false, error: "invalid_url" });
      return;
    }
    if (u.protocol === "http:" && !allowInsecure) {
      res.status(400).json({ ok: false, error: "insecure_url_not_allowed" });
      return;
    }

    stage = "fetch_upstream";
    const upstreamRes = await fetch(url, {
      method: "GET",
      headers: {
        Accept: "application/json",
        Authorization: `Bearer ${token}`,
      },
    });

    if (!upstreamRes.ok) {
      res.status(502).json({ ok: false, error: "upstream_unreachable", status: upstreamRes.status });
      return;
    }

    stage = "parse_upstream_json";
    let data;
    try {
      data = await upstreamRes.json();
    } catch {
      res.status(502).json({ ok: false, error: "invalid_upstream_json" });
      return;
    }
    if (!data || typeof data !== "object") {
      res.status(502).json({ ok: false, error: "invalid_upstream_payload" });
      return;
    }

    stage = "normalize";
    const normalized = applyExpectedTotals(normalizeUpstream(data));
    delete normalized.debug;
    normalized.workers = [];

    res.setHeader("Cache-Control", "no-store");
    res.status(200).json(normalized);
  } catch (e) {
    const debug = (process.env.DEBUG_PROXY || "").toLowerCase() === "true";
    res.status(502).json({
      ok: false,
      error: "proxy_error",
      stage,
      ...(debug ? { detail: String(e && e.message ? e.message : e) } : {}),
    });
  }
};


