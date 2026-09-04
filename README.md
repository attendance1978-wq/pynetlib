# pynetlib

A lightweight TCP/UDP networking library for Python.

**Version:** 25.0

## Features

- **Async TCP** — `Server` / `Client`, built on `asyncio`
- **Sync TCP** — `SyncServer` / `SyncClient`, thread-based
- **UDP** — `UdpServer` / `UdpClient`
- **Packets** — length-prefixed framing (`PacketCodec`) plus a JSON-header `Packet` format
- **Pipeline** — Netty-style handler chain (`Pipeline`, `Handler`, `Context`) for composable read/write processing
- **Request/response** — `send_and_receive` (sync) and `request()` (async) correlate replies to requests over a single connection
- **Utils** — local/public IP lookup, port scanning, port availability checks

## Install

Copy the `pynetlib` package into your project (e.g. under `lib/`).

## Quick start

### Sync server

```python
import lib.pynetlib.main as net

server = net.create_server("0.0.0.0", 8888, sync=True)
server.on_message = lambda cid, data: f"Echo: {data.decode()}".encode()
server.start()
```

### Sync client

```python
import lib.pynetlib.main as net

client = net.create_client("127.0.0.1", 8888, sync=True)
client.connect()
response = client.send_and_receive(b"Hello!")
```

### Async server

```python
import asyncio
from pynetlib import Server

async def main():
    server = Server("0.0.0.0", 8888)
    server.on_connect = lambda conn: print("client connected:", conn.remote_addr)
    await server.start()

asyncio.run(main())
```

### Async client

```python
import asyncio
from pynetlib import Client

async def main():
    client = Client("127.0.0.1", 8888)
    await client.connect()
    await client.send(b"hello")
    response = await client.request(b"ping")

asyncio.run(main())
```

### UDP

```python
from pynetlib import UdpServer, UdpClient

server = UdpServer("0.0.0.0", 9999)
server.on_message = lambda data, addr: b"ack"
server.start()

client = UdpClient("127.0.0.1", 9999)
client.connect()
reply = client.send_and_receive(b"ping")
```

## Module overview

| Module | Contents |
|---|---|
| `server.py` | `Server` (async), `SyncServer` (threaded) |
| `client.py` | `Client` (async), `SyncClient` (threaded) |
| `udp.py` | `UdpServer`, `UdpClient` |
| `connection.py` | `Connection` — per-connection read loop, write lock, stats |
| `pipeline.py` | `Pipeline`, `Handler`, `Context` — composable handler chain |
| `packet.py` | `Packet`, `PacketStatus`, `PacketCodec` — framing and JSON-header packets |
| `utils.py` | IP/hostname/port helper functions |
| `main.py` | `create_server`, `create_client`, `create_udp_server`, `create_udp_client`, `create_pipeline` convenience factories |

## Version history

- **25.0** — current release
- 2.0.0 — previous release

## Examples

See `reference_client.txt` and `reference_server.txt` for minimal sync usage.
