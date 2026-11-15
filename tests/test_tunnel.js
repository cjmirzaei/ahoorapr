"use strict";

const http = require("http");
const net = require("net");
const { spawn } = require("child_process");
const WebSocket = require("ws");

const TEST_PORT = 5891;
const ECHO_PORT = 5892;
const TEST_UUID = "e2d84c1f-4b3a-4f5c-8d2e-1a9b8c7d6e5f";
const TEST_UUID_BYTES = Buffer.from(TEST_UUID.replace(/-/g, ""), "hex");
const INVALID_UUID_BYTES = Buffer.from("00000000000000000000000000000000", "hex");

async function main() {
  console.log("Starting verification test...");

  // 1. Start echo server
  const echoServer = net.createServer((socket) => {
    socket.on("data", (chunk) => {
      socket.write(chunk);
    });
  });
  await new Promise((resolve) => echoServer.listen(ECHO_PORT, "127.0.0.1", resolve));
  console.log("Echo server running on port " + ECHO_PORT);

  // 2. Start server under test
  const child = spawn(process.execPath, ["index.js"], {
    cwd: process.cwd(),
    env: {
      ...process.env,
      PORT: String(TEST_PORT),
      WS_PATH: "/ws",
      UUID: TEST_UUID,
    },
    stdio: ["ignore", "pipe", "pipe"],
  });

  child.stdout.on("data", (d) => process.stdout.write("[child stdout] " + d));
  child.stderr.on("data", (d) => process.stderr.write("[child stderr] " + d));

  // Wait for server to listen
  await new Promise((resolve) => setTimeout(resolve, 800));

  try {
    // 3. Test HTTP /health
    const healthRes = await new Promise((resolve, reject) => {
      http.get(`http://127.0.0.1:${TEST_PORT}/health`, (res) => {
        let data = "";
        res.on("data", (c) => (data += c));
        res.on("end", () => resolve({ status: res.statusCode, body: data }));
      }).on("error", reject);
    });
    if (healthRes.status !== 200 || healthRes.body !== "OK") {
      throw new Error(`Health check failed: ${JSON.stringify(healthRes)}`);
    }
    console.log("PASS: /health endpoint returns 200 OK");

    // 4. Test HTTP /
    const rootRes = await new Promise((resolve, reject) => {
      http.get(`http://127.0.0.1:${TEST_PORT}/`, (res) => {
        let data = "";
        res.on("data", (c) => (data += c));
        res.on("end", () => resolve({ status: res.statusCode, body: data }));
      }).on("error", reject);
    });
    if (rootRes.status !== 200 || rootRes.body !== "ahoorapr running") {
      throw new Error(`Root check failed: ${JSON.stringify(rootRes)}`);
    }
    console.log("PASS: / endpoint returns 200 ahoorapr running");

    // 5. Test invalid UUID rejection
    const invalidWsResult = await new Promise((resolve) => {
      const ws = new WebSocket(`ws://127.0.0.1:${TEST_PORT}/ws`);
      ws.on("open", () => {
        const frame = Buffer.concat([
          Buffer.from([0x00]), // version
          INVALID_UUID_BYTES,  // 16 bytes invalid UUID
          Buffer.from([0x00]), // addons len 0
          Buffer.from([0x01]), // command 0x01 = TCP
          Buffer.from([0x17, 0x04]), // port 5892 (0x1704)
          Buffer.from([0x01]), // IPv4
          Buffer.from([127, 0, 0, 1]),
          Buffer.from("bad-ping"),
        ]);
        ws.send(frame);
      });
      ws.on("close", (code) => {
        resolve({ closed: true, code });
      });
      ws.on("error", () => {});
    });
    if (invalidWsResult.code !== 1008) {
      throw new Error(`Expected close code 1008 for invalid UUID, got ${invalidWsResult.code}`);
    }
    console.log("PASS: Invalid UUID correctly rejected with code 1008");

    // 6. Test valid connection & bidirectional pipe through echo server
    const echoResult = await new Promise((resolve, reject) => {
      const ws = new WebSocket(`ws://127.0.0.1:${TEST_PORT}/ws`);
      let headerReceived = false;
      const chunks = [];

      ws.on("open", () => {
        const portBuf = Buffer.alloc(2);
        portBuf.writeUInt16BE(ECHO_PORT, 0);

        const frame = Buffer.concat([
          Buffer.from([0x00]), // version
          TEST_UUID_BYTES,     // valid 16-byte UUID
          Buffer.from([0x00]), // addons len
          Buffer.from([0x01]), // command = TCP
          portBuf,             // port
          Buffer.from([0x01]), // IPv4
          Buffer.from([127, 0, 0, 1]), // 127.0.0.1
          Buffer.from("initial-payload"),
        ]);
        ws.send(frame);
      });

      ws.on("message", (msg) => {
        const buf = Buffer.isBuffer(msg) ? msg : Buffer.from(msg);
        if (!headerReceived) {
          if (buf.length >= 2 && buf[0] === 0x00 && buf[1] === 0x00) {
            headerReceived = true;
            // Send additional message over established pipe
            ws.send(Buffer.from("follow-up-test"));
            if (buf.length > 2) {
              chunks.push(buf.subarray(2).toString());
            }
          }
        } else {
          chunks.push(buf.toString());
          if (chunks.join("").includes("follow-up-test")) {
            ws.close();
            resolve({ headerReceived, text: chunks.join("") });
          }
        }
      });

      ws.on("error", reject);
      setTimeout(() => reject(new Error("Timeout waiting for echo")), 4000);
    });

    if (!echoResult.headerReceived || !echoResult.text.includes("initial-payload") || !echoResult.text.includes("follow-up-test")) {
      throw new Error(`Echo verification failed: ${JSON.stringify(echoResult)}`);
    }
    console.log("PASS: Valid handshake, response header [0x00, 0x00], and bidirectional relay verified!");

    console.log("ALL TESTS PASSED SUCCESSFULLY!");
  } finally {
    child.kill("SIGTERM");
    echoServer.close();
  }
}

main().catch((err) => {
  console.error("TEST FAILED:", err);
  process.exit(1);
});
