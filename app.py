#!/usr/bin/env python3
import asyncio
import base64
import hashlib
import json
import os
import socket
import struct
import sys
import urllib.parse
import urllib.request

PORT = int(os.environ.get("PORT", "5000"))
WS_PATH = os.environ.get("WS_PATH", "/ws")
if not WS_PATH.startswith("/"):
    WS_PATH = "/" + WS_PATH

UUID_STR = os.environ.get("UUID") or os.environ.get("AUTH_TOKEN") or "d342d11e-d424-4583-b36e-524ab1f0afa4"
UUID_STR = UUID_STR.strip().lower()
UUID_BYTES = bytes.fromhex(UUID_STR.replace("-", ""))
WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

SERVER_DOMAIN = os.environ.get("SERVER_DOMAIN", "newdogh.infrlo.com").strip()
PROJECT_URL = os.environ.get("PROJECT_URL", f"https://{SERVER_DOMAIN}").strip()
AUTO_ACCESS = os.environ.get("AUTO_ACCESS", "true").lower() in ("true", "1", "yes")

TG_BOT_TOKEN = os.environ.get("TG_BOT_TOKEN", "").strip()
TG_CHAT_ID = os.environ.get("TG_CHAT_ID", "").strip()

def log(*args):
    print("[ahoorapr]", *args, flush=True)

class WSConn:
    def __init__(self, reader, writer):
        self.r = reader
        self.w = writer
        self.buf = bytearray()

    async def read_exact(self, n):
        while len(self.buf) < n:
            chunk = await self.r.read(n - len(self.buf))
            if not chunk:
                raise ConnectionError("connection closed")
            self.buf += chunk
        out = bytes(self.buf[:n])
        del self.buf[:n]
        return out

    async def send_frame(self, payload, opcode=0x2):
        n = len(payload)
        if n < 126:
            head = struct.pack("!BB", 0x80 | opcode, n)
        elif n < 65536:
            head = struct.pack("!BBH", 0x80 | opcode, 126, n)
        else:
            head = struct.pack("!BBQ", 0x80 | opcode, 127, n)
        self.w.write(head + payload)
        await self.w.drain()

    async def read_message(self):
        payload = bytearray()
        opcode = None
        while True:
            h = await self.read_exact(2)
            fin = h[0] & 0x80
            op = h[0] & 0x0F
            masked = h[1] & 0x80
            ln = h[1] & 0x7F
            if ln == 126:
                ln = struct.unpack("!H", await self.read_exact(2))[0]
            elif ln == 127:
                ln = struct.unpack("!Q", await self.read_exact(8))[0]
            mask = await self.read_exact(4) if masked else None
            data = await self.read_exact(ln) if ln else b""
            if mask:
                data = bytes(b ^ mask[i % 4] for i, b in enumerate(data))
            if op == 0x9:
                await self.send_frame(data, opcode=0xA)
                continue
            if op == 0x8:
                raise ConnectionError("close frame")
            if opcode is None:
                opcode = op
            payload += data
            if fin:
                return opcode, bytes(payload)

async def handle_client(reader, writer):
    try:
        raw = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=15)
    except Exception:
        writer.close()
        return

    head = raw.decode("latin1", errors="ignore")
    lines = head.split("\r\n")
    try:
        parts = lines[0].split(" ")
        method, path = parts[0], parts[1]
    except Exception:
        writer.close()
        return

    headers = {}
    for ln in lines[1:]:
        if ":" in ln:
            k, v = ln.split(":", 1)
            headers[k.strip().lower()] = v.strip()

    is_ws = headers.get("upgrade", "").lower() == "websocket"
    clean_path = path.split("?")[0]

    if clean_path == "/health":
        writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 2\r\nConnection: close\r\n\r\nOK")
        await writer.drain()
        writer.close()
        return

    if clean_path == "/":
        writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 16\r\nConnection: close\r\n\r\nahoorapr running")
        await writer.drain()
        writer.close()
        return

    if not (is_ws and clean_path == WS_PATH):
        writer.write(b"HTTP/1.1 404 Not Found\r\nConnection: close\r\n\r\n")
        await writer.drain()
        writer.close()
        return

    key = headers.get("sec-websocket-key", "")
    accept = base64.b64encode(hashlib.sha1((key + WS_GUID).encode()).digest()).decode()
    writer.write(
        ("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: " + accept + "\r\n\r\n").encode()
    )
    await writer.drain()

    ws = WSConn(reader, writer)
    ws.buf += reader._buffer if hasattr(reader, "_buffer") else b""

    try:
        _, msg = await ws.read_message()
        if len(msg) < 22:
            return
        ver = msg[0]
        if ver != 0x00:
            return
        if UUID_BYTES and msg[1:17] != UUID_BYTES:
            return
        i = 17
        addons_len = msg[i]
        i += 1 + addons_len
        if i >= len(msg):
            return
        cmd = msg[i]
        if cmd != 0x01:
            return
        i += 1
        target_port = struct.unpack("!H", msg[i : i + 2])[0]
        i += 2
        atyp = msg[i]
        i += 1
        if atyp == 1:
            target_host = ".".join(str(b) for b in msg[i : i + 4])
            i += 4
        elif atyp == 2:
            ln = msg[i]
            i += 1
            target_host = msg[i : i + ln].decode("ascii", errors="ignore")
            i += ln
        elif atyp == 3:
            raw6 = msg[i : i + 16]
            i += 16
            target_host = ":".join("%x" % struct.unpack("!H", raw6[j : j + 2])[0] for j in range(0, 16, 2))
        else:
            return

        payload = msg[i:]
        t_reader, t_writer = await asyncio.open_connection(target_host, target_port)

        sock = t_writer.get_extra_info("socket")
        if sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        await ws.send_frame(bytes([0x00, 0x00]))
        if payload:
            t_writer.write(payload)
            await t_writer.drain()

        async def forward_remote_to_ws():
            try:
                while True:
                    data = await t_reader.read(32768)
                    if not data:
                        break
                    await ws.send_frame(data)
            except Exception:
                pass

        up_task = asyncio.create_task(forward_remote_to_ws())
        try:
            while True:
                _, data = await ws.read_message()
                t_writer.write(data)
                await t_writer.drain()
        except Exception:
            pass
        finally:
            up_task.cancel()
            t_writer.close()
    except Exception:
        pass
    finally:
        try:
            writer.close()
        except Exception:
            pass

def print_server_summary():
    log("=" * 60)
    log("AHOORAPR SERVICE ACTIVE")
    log("=" * 60)
    log(f"Domain : {SERVER_DOMAIN}")
    log(f"Port   : 443 (Internal forward -> {PORT})")
    log(f"Path   : {WS_PATH}")
    log(f"UUID   : {UUID_STR}")
    log(f"TLS    : Enabled (SNI: {SERVER_DOMAIN})")
    log(f"Ping   : {PROJECT_URL}/health (Interval: 240s)")
    log("=" * 60)

async def keepalive_loop():
    if not AUTO_ACCESS:
        return
    await asyncio.sleep(15)
    ping_url = f"{PROJECT_URL}/health" if not PROJECT_URL.endswith("/") else f"{PROJECT_URL}health"
    log(f"Keepalive watcher active -> {ping_url}")
    while True:
        try:
            req = urllib.request.Request(ping_url, headers={"User-Agent": "ahoorapr-keepalive/1.0"})
            with urllib.request.urlopen(req, timeout=10) as resp:
                status = resp.getcode()
                log(f"Keepalive ping status: {status}")
        except Exception as e:
            log(f"Keepalive ping tick: {e}")
        await asyncio.sleep(240)

async def telegram_notify():
    if not (TG_BOT_TOKEN and TG_CHAT_ID):
        return
    text = (
        f"Server Started:\n"
        f"Domain: {SERVER_DOMAIN}\n"
        f"Port: 443\n"
        f"Path: {WS_PATH}\n"
        f"UUID: {UUID_STR}"
    )
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    payload = urllib.parse.urlencode({"chat_id": TG_CHAT_ID, "text": text}).encode()
    try:
        req = urllib.request.Request(url, data=payload, method="POST")
        with urllib.request.urlopen(req, timeout=10) as resp:
            log("Telegram startup notification dispatched")
    except Exception as e:
        log("Telegram notify notice:", e)

async def main():
    server = await asyncio.start_server(handle_client, "0.0.0.0", PORT)
    print_server_summary()

    asyncio.create_task(keepalive_loop())
    asyncio.create_task(telegram_notify())

    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass
