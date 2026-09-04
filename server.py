"""
Server module - TCP Server implementation
"""

import asyncio
import socket
import threading
from typing import Optional, Dict, Any, Callable, List

from .connection import Connection
from .pipeline import Pipeline

class Server:
    """TCP Server (Async)"""
    
    def __init__(self, host: str = '0.0.0.0', port: int = 8888,
                 pipeline_factory: Optional[Callable[[], Pipeline]] = None):
        self.host = host
        self.port = port
        self.pipeline_factory = pipeline_factory or (lambda: Pipeline())
        self._server: Optional[asyncio.Server] = None
        self._connections: List[Connection] = []
        self._running = False
        self._lock = asyncio.Lock()
        
        self.stats = {
            'total_connections': 0,
            'active_connections': 0,
            'bytes_received': 0,
            'bytes_sent': 0
        }
        
        self.on_connect: Optional[Callable] = None
        self.on_disconnect: Optional[Callable] = None
        self.on_error: Optional[Callable] = None
    
    async def start(self):
        """Start the server"""
        if self._running:
            return
        
        self._server = await asyncio.start_server(
            self._handle_client,
            self.host,
            self.port
        )
        self._running = True
        print(f"✅ Server started on {self.host}:{self.port}")
        
        async with self._server:
            await self._server.serve_forever()
    
    async def _handle_client(self, reader, writer):
        pipeline = self.pipeline_factory()
        conn = Connection(reader, writer, pipeline)
        
        async with self._lock:
            self._connections.append(conn)
            self.stats['total_connections'] += 1
            self.stats['active_connections'] += 1
        
        try:
            if self.on_connect:
                self.on_connect(conn)
            await conn.start()
        finally:
            async with self._lock:
                if conn in self._connections:
                    self._connections.remove(conn)
                    self.stats['active_connections'] = max(0, self.stats['active_connections'] - 1)
            if self.on_disconnect:
                self.on_disconnect(conn)
    
    async def stop(self):
        """Stop the server"""
        if not self._running:
            return
        self._running = False
        for conn in self._connections[:]:
            try:
                await conn.close()
            except:
                pass
        if self._server:
            self._server.close()
            await self._server.wait_closed()
        print("🛑 Server stopped")
    
    def is_running(self) -> bool:
        """Check if server is running"""
        return self._running

class SyncServer:
    """TCP Server (Synchronous)"""
    
    def __init__(self, host: str = '0.0.0.0', port: int = 8888):
        self.host = host
        self.port = port
        self.socket: Optional[socket.socket] = None
        self.running = False
        self.clients: Dict[str, socket.socket] = {}
        self._lock = threading.Lock()
        
        self.stats = {
            'total_connections': 0,
            'messages_received': 0,
            'messages_sent': 0
        }
        
        self.on_connect: Optional[Callable] = None
        self.on_message: Optional[Callable[[str, bytes], Optional[bytes]]] = None
        self.on_disconnect: Optional[Callable] = None
        self.on_error: Optional[Callable] = None
    
    def start(self) -> bool:
        """Start the server"""
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.socket.bind((self.host, self.port))
            self.socket.listen(100)
            self.running = True
            print(f"✅ Server started on {self.host}:{self.port}")
            
            accept_thread = threading.Thread(target=self._accept_loop, daemon=True)
            accept_thread.start()
            return True
        except Exception as e:
            print(f"❌ Failed to start server: {e}")
            return False
    
    def _accept_loop(self):
        """Accept connections loop"""
        while self.running:
            try:
                client_socket, addr = self.socket.accept()
                client_id = f"{addr[0]}:{addr[1]}"
                
                with self._lock:
                    self.clients[client_id] = client_socket
                    self.stats['total_connections'] += 1
                
                if self.on_connect:
                    self.on_connect(client_id, addr)
                
                client_thread = threading.Thread(
                    target=self._handle_client,
                    args=(client_id, client_socket),
                    daemon=True
                )
                client_thread.start()
            except Exception as e:
                if self.running and self.on_error:
                    self.on_error(e)
    
    def _handle_client(self, client_id: str, client_socket: socket.socket):
        """Handle a client connection"""
        from .packet import PacketCodec
        
        buffer = bytearray()
        while self.running and client_id in self.clients:
            try:
                client_socket.settimeout(1.0)
                data = client_socket.recv(4096)
                if not data:
                    break
                
                buffer.extend(data)
                while True:
                    payload = PacketCodec.decode(buffer)
                    if payload is None:
                        break
                    
                    self.stats['messages_received'] += 1
                    
                    # Check if this is a request with ID
                    payload_str = payload.decode('utf-8', errors='ignore')
                    is_request = False
                    request_id = None
                    actual_data = payload
                    
                    if payload_str.startswith("REQ:"):
                        parts = payload_str.split(":", 2)
                        if len(parts) >= 3:
                            is_request = True
                            request_id = parts[1]
                            actual_data = parts[2].encode('utf-8')
                    
                    if self.on_message:
                        try:
                            response = self.on_message(client_id, actual_data)
                            if response:
                                # If it was a request, send response with ID
                                if is_request and request_id:
                                    response_data = f"RESP:{request_id}:".encode() + response
                                    self.send(client_id, response_data)
                                else:
                                    self.send(client_id, response)
                        except Exception as e:
                            if self.on_error:
                                self.on_error(e)
            except socket.timeout:
                continue
            except Exception as e:
                if self.on_error:
                    self.on_error(e)
                break
        
        self.disconnect(client_id)
    
    def send(self, client_id: str, data: bytes) -> bool:
        """Send data to a client"""
        from .packet import PacketCodec
        
        if client_id not in self.clients:
            return False
        
        try:
            packet = PacketCodec.encode(data)
            self.clients[client_id].send(packet)
            self.stats['messages_sent'] += 1
            return True
        except Exception as e:
            if self.on_error:
                self.on_error(e)
            self.disconnect(client_id)
            return False
    
    def broadcast(self, data: bytes) -> int:
        """Broadcast to all clients"""
        sent = 0
        for client_id in list(self.clients.keys()):
            if self.send(client_id, data):
                sent += 1
        return sent
    
    def disconnect(self, client_id: str):
        """Disconnect a client"""
        with self._lock:
            if client_id not in self.clients:
                return
            try:
                self.clients[client_id].close()
            except:
                pass
            del self.clients[client_id]
        
        if self.on_disconnect:
            self.on_disconnect(client_id)
    
    def stop(self):
        """Stop the server"""
        self.running = False
        for client_id in list(self.clients.keys()):
            self.disconnect(client_id)
        if self.socket:
            try:
                self.socket.close()
            except:
                pass
        print("🛑 Server stopped")