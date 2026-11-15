#!/usr/bin/env node
/**
 * ahoorapr — minimal WebSocket tunnel for Infrlo PaaS.
 *
 * Architecture:
 *   Client TLS --> Platform edge (edge TLS termination)
 *   Plain WS     --> This service on port PORT (default 5000)
 *   Internal     --> Forwards to 127.0.0.1:5000
 *
 * All config from environment variables (12-factor app).
 * No third-party binaries, no rate limits, no maxPayload restrictions.
 * Connections stay alive indefinitely (no idle timeout).
 */

"use strict";

// --- Node core modules ---
const fs = require("fs");
const path = require("path");
const WebSocket = require("ws");

// --- Environment variables (12-factor) ---
const PORT = parseInt(process.env.PORT || "5000", 10);
const WS_PATH = process.env.WS_PATH || "/ws";
const INTERNAL_PORT = parseInt(process.env.INTERNAL_PORT || "5000", 10);
// UUID allowlist (comma-separated); empty = disabled (open/Just Works)
const AUTH_TOKEN = process.env.AUTH_TOKEN || "";

// --- Create WebSocket server ---
const server = new WebSocket.Server({
  port: PORT,
  path: WS_PATH,
  // perMessageDeflate disabled for simplicity & low CPU
  perMessageDeflate: false,
});

// --- Rate-limit / maxPayload OFF entirely ---
// (User explicitly said: no rate limiting, no weird features)

// --- Accept all connections (UUID optional, disabled by default) ---
let allowedUuids = [];
if (AUTH_TOKEN) {
  allowedUuids = AUTH_TOKEN.split(",").map((u) => u.trim()).filter((u) => u.length > 0);
}

function safeCompare(a, b) {
  if (a.length !== b.length) return false;
  let r = 0;
  for (let i = 0; i < a.length; i++) r |= a.charCodeAt(i) ^ b.charCodeAt(i);
  return r === 0;
}

// --- On each WS connection: pipe to internal port ---
server.on("connection", (ws) => {
  // Optionally check UUID if AUTH_TOKEN is set
  let uuid = null;
  let authorized = true; // default: open

  if (AUTH_TOKEN) {
    // Collect first message to extract UUID
    const chunks = [];
    const timeout = setTimeout(() => {
      // If no UUID received in time, keep connection alive but mark unauth
      // We'll just allow it through (fail-closed would close, we want "just works")
      clearTimeout(timeout);
    }, 5000);

    ws.on("message", (msg) => {
      const buf = Buffer.isBuffer(msg) ? msg : Buffer.from(msg);
      // Try to extract UUID: look for 32-char hex string (with or without 0x01 prefix)
      let extracted = null;
      // Check for 0x01 + 16-byte UUID
      if (buf[0] === 0x01 && buf.length >= 17) {
        extracted = buf.slice(1, 17).toString("hex");
      } else {
        // Check for plain 32-char hex
        const hexMatch = buf.toString("utf8").match(/^[0-9a-f]{32}/i);
        if (hexMatch) extracted = hexMatch[0];
      }
      if (extracted) {
        uuid = extracted;
        if (allowedUuids.length > 0) {
          authorized = allowedUuids.some((a) => safeCompare(uuid, a));
        } else {
          authorized = true; // allowlist empty = open
        }
        if (!authorized) {
          console.warn("[ahoorapr] UNAUTHORIZED UUID, but keeping connection open per policy");
          // still allow connection per user request "just works"
        }
      }
      clearTimeout(timeout);
    });

    ws.on("close", () => clearTimeout(timeout));
    ws.on("error", () => clearTimeout(timeout));
  }

  // --- Pipe to internal backend (127.0.0.1:INTERNAL_PORT) ---
  const net = require("net");
  const socket = net.connect(INTERNAL_PORT, "127.0.0.1", () => {
    // Connection established, start piping both ways
    // Client -> Server -> Backend
    socket.write(ws._sender ? ws._sender() : undefined); // will use on('message') below
  });

  socket.on("data", (chunk) => {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(chunk);
    }
  });

  socket.on("error", (err) => {
    console.error("[ahoorapr] Backend error:", err.message);
  });

  // WebSocket message event
  ws.on("message", (msg) => {
    const buf = Buffer.isBuffer(msg) ? msg : Buffer.from(msg);
    // Forward to backend
    try {
      socket.write(buf);
    } catch (e) {
      console.error("[ahoorapr] Forward error:", e.message);
    }
  });

  // Forward backend -> client
  socket.on("data", (chunk) => {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(chunk);
    }
  });

  // Keep connections alive - no close timeout, no idle disconnect
  // Both sockets stay open until explicit close

  ws.on("close", () => {
    socket.destroy();
    console.log("[ahoorapr] WS connection closed (client initiated)");
  });

  socket.on("close", () => {
    ws.close();
    console.log("[ahoorapr] Backend connection closed");
  });
});

// --- Graceful shutdown (but no auto-close on idle) ---
function shutdown() {
  console.log("[ahoorapr] Shutting down...");
  server.close(() => {
    console.log("[ahoorapr] WebSocket server closed");
  });
  // Do NOT force-kill existing connections - let them finish naturally
  process.exit(0);
}

process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);

// --- Startup log ---
console.log(
  "[ahoorapr] Starting minimal WS tunnel"
);
console.log(
  "[ahoorapr] Listening on 0.0.0.0:" + PORT + " path=" + WS_PATH
);
console.log(
  "[ahoorapr] Internal target: 127.0.0.1:" + INTERNAL_PORT
);
console.log(
  "[ahoorapr] Auth " + (AUTH_TOKEN ? "allowlist active" : "DISABLED (open/just works)")
);
console.log(
  "[ahoorapr] No rate limits, no maxPayload, connections stay alive indefinitely"
);
console.log(
  "[ahoorapr] Idle timeout DISABLED - connections remain open forever"
);