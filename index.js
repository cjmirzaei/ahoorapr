#!/usr/bin/env node
"use strict";

const http = require("http");
const net = require("net");
const crypto = require("crypto");
const { WebSocketServer } = require("ws");

const PORT = parseInt(process.env.PORT, 10) || 5000;
const WS_PATH = process.env.WS_PATH || "/ws";
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

function parseUuidList(raw) {
  const seen = new Set();
  const list = String(raw || "")
    .split(",")
    .map((v) => v.trim().toLowerCase())
    .filter((v) => UUID_RE.test(v))
    .filter((v) => {
      if (seen.has(v)) return false;
      seen.add(v);
      return true;
    });

  if (list.length === 0) {
    list.push(crypto.randomUUID().toLowerCase());
  }
  return list;
}

const UUID_STRINGS = parseUuidList(process.env.UUID || process.env.AUTH_TOKEN);
const UUID_BYTES = UUID_STRINGS.map((id) => Buffer.from(id.replace(/-/g, ""), "hex"));

function isValidUuid(clientBytes) {
  if (!Buffer.isBuffer(clientBytes) || clientBytes.length !== 16) return false;
  for (const expected of UUID_BYTES) {
    if (crypto.timingSafeEqual(clientBytes, expected)) {
      return true;
    }
  }
  return false;
}

const server = http.createServer((req, res) => {
  const url = new URL(req.url || "/", `http://${req.headers.host || "localhost"}`);

  if (url.pathname === "/health") {
    res.writeHead(200, { "Content-Type": "text/plain; charset=utf-8" });
    return res.end("OK");
  }

  if (url.pathname === "/") {
    res.writeHead(200, { "Content-Type": "text/plain; charset=utf-8" });
    return res.end("ahoorapr running");
  }

  res.writeHead(404, { "Content-Type": "text/plain; charset=utf-8" });
  res.end("Not Found");
});

function handleTunnel(ws) {
  ws.on("error", () => {});

  ws.once("message", (data, isBinary) => {
    if (!isBinary) {
      ws.close(1008, "Binary required");
      return;
    }

    const buf = Buffer.isBuffer(data) ? data : Buffer.from(data);
    if (buf.length < 22) {
      ws.close(1008, "Invalid payload header");
      return;
    }

    let offset = 0;
    const version = buf[offset++];
    if (version !== 0x00) {
      ws.close(1008, "Unsupported version");
      return;
    }

    const clientUuidBytes = buf.subarray(offset, offset + 16);
    offset += 16;
    if (!isValidUuid(clientUuidBytes)) {
      ws.close(1008, "Unauthorized");
      return;
    }

    const addonsLength = buf[offset++];
    offset += addonsLength;
    if (offset >= buf.length) {
      ws.close(1008, "Truncated header");
      return;
    }

    const command = buf[offset++];
    if (command !== 0x01) {
      ws.close(1008, "Only TCP supported");
      return;
    }

    if (offset + 2 > buf.length) {
      ws.close(1008, "Truncated port");
      return;
    }
    const targetPort = buf.readUInt16BE(offset);
    offset += 2;

    const addressType = buf[offset++];
    let targetAddress;
    if (addressType === 0x01) {
      if (offset + 4 > buf.length) {
        ws.close(1008, "Truncated IPv4");
        return;
      }
      targetAddress = `${buf[offset]}.${buf[offset + 1]}.${buf[offset + 2]}.${buf[offset + 3]}`;
      offset += 4;
    } else if (addressType === 0x02) {
      const len = buf[offset++];
      if (offset + len > buf.length) {
        ws.close(1008, "Truncated domain");
        return;
      }
      targetAddress = buf.subarray(offset, offset + len).toString("ascii");
      offset += len;
    } else if (addressType === 0x03) {
      if (offset + 16 > buf.length) {
        ws.close(1008, "Truncated IPv6");
        return;
      }
      targetAddress = buf.subarray(offset, offset + 16).toString("hex").match(/.{1,4}/g).join(":");
      offset += 16;
    } else {
      ws.close(1008, "Unknown address type");
      return;
    }

    const initialPayload = buf.subarray(offset);

    const tcp = net.connect({ port: targetPort, host: targetAddress }, () => {
      tcp.setKeepAlive(true, 10000);
      tcp.setNoDelay(true);

      ws.send(Buffer.from([0x00, 0x00]));

      if (initialPayload.length > 0) {
        tcp.write(initialPayload);
      }

      tcp.on("data", (chunk) => {
        if (ws.readyState !== ws.OPEN) return;
        if (ws.bufferedAmount >= 512 * 1024) tcp.pause();
        ws.send(chunk, { binary: true }, () => {
          if (tcp.isPaused()) tcp.resume();
        });
      });
    });

    tcp.on("error", () => {
      try {
        ws.close();
      } catch {}
    });

    tcp.on("close", () => {
      try {
        ws.close();
      } catch {}
    });

    ws.on("message", (chunk) => {
      const dataBuffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
      if (!tcp.writable) return;
      if (!tcp.write(dataBuffer)) {
        ws.pause();
        tcp.once("drain", () => ws.resume());
      }
    });

    ws.on("close", () => {
      tcp.destroy();
    });

    ws.on("error", () => {
      tcp.destroy();
    });
  });
}

const wss = new WebSocketServer({
  noServer: true,
  clientTracking: false,
  perMessageDeflate: false,
});

server.on("upgrade", (req, socket, head) => {
  let pathname;
  try {
    const url = new URL(req.url || "/", `http://${req.headers.host || "localhost"}`);
    pathname = url.pathname;
  } catch {
    socket.destroy();
    return;
  }

  if (pathname !== WS_PATH) {
    socket.write("HTTP/1.1 404 Not Found\r\nConnection: close\r\n\r\n");
    socket.destroy();
    return;
  }

  wss.handleUpgrade(req, socket, head, (ws) => {
    handleTunnel(ws);
  });
});

function shutdown() {
  server.close(() => {
    process.exit(0);
  });
}

process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);

server.listen(PORT, () => {
  console.log(`[ahoorapr] Server listening on port ${PORT}`);
  console.log(`[ahoorapr] Path: ${WS_PATH}`);
  console.log(`[ahoorapr] UUIDs configured: ${UUID_STRINGS.length}`);
});
