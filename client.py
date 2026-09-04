"""
Client module - TCP Client implementation
"""

import asyncio
import socket
import threading
import time
from typing import Optional, Dict, Any, Callable

from .connection import Connection
from .pipeline import Pipeline

class Client:
    """TCP Client (Async)"""
    
    def __init__(self, host: str = '127.0.0.1', port: int = 8888,
                 pipeline_factory: Optional[Callable[[], Pipeline]] = None):
        self.host = host
        self.port = port
        self.pipeline_factory = pipeline_factory or (lambda: Pipeline())
        self.connection: Optional[Connection] = None
        self._connected = False
        self._running = False
        self._response_futures = {}
        self._counter = 0
        
        self.stats = {
            'messages_sent': 0,
            'messages_received': 0,
            'reconnects': 0
        }
        
        self.on_connect: Optional[Callable] = None
        self.on_message: Optional[Callable[[bytes], Optional[bytes]]] = None
        self.on_disconnect: Optional[Callable] = None
        self.on_error: Optional[Callable] = None
        
        self.auto_reconnect = False
        self.reconnect_delay = 5
        self.max_reconnects = 3
        self._reconnect_count = 0
    
    async def connect(self) -> bool:
        """Connect to the server"""
        try:
            reader, writer = await asyncio.open_connection(self.host, self.port)
            pipeline = self.pipeline_factory()
            self.connection = Connection(reader, writer, pipeline)
            self._connected = True
            self._running = True
            self._reconnect_count = 0
            
            print(f"✅ Connected to {self.host}:{self.port}")
            
            if self.on_connect:
                self.on_connect()
            
            asyncio.create_task(self._connection_loop())
            return True
            
        except Exception as e:
            print(f"❌ Connection failed: {e}")
            if self.on_error:
                self.on_error(e)
            return False
    
    async def _connection_loop(self):
        """Main connection loop"""
        try:
            if self.connection:
                await self.connection.start()
        except Exception as e:
            if self.on_error:
                self.on_error(e)
        finally:
            await self.disconnect()
    
    async def send(self, data: bytes) -> bool:
        """Send data to the server"""
        if not self.connection or self.connection.is_closed():
            return False
        
        try:
            result = await self.connection.write(data)
            if result:
                self.stats['messages_sent'] += 1
            return result
        except Exception as e:
            if self.on_error:
                self.on_error(e)
            return False
    
    async def request(self, data: bytes, timeout: float = 5.0) -> Optional[bytes]:
        """Send a request and wait for a response"""
        if not self.connection or self.connection.is_closed():
            return None
        
        self._counter += 1
        request_id = str(self._counter)
        future = asyncio.Future()
        self._response_futures[request_id] = future
        
        try:
            # Send with request ID in the data
            data_with_id = f"REQ:{request_id}:".encode() + data
            sent = await self.connection.write(data_with_id)
            if not sent:
                self._response_futures.pop(request_id, None)
                return None
            
            try:
                response = await asyncio.wait_for(future, timeout)
                return response
            except asyncio.TimeoutError:
                return None
            finally:
                self._response_futures.pop(request_id, None)
        except Exception as e:
            self._response_futures.pop(request_id, None)
            if self.on_error:
                self.on_error(e)
            return None
    
    async def disconnect(self):
        """Disconnect from the server"""
        if not self._connected:
            return
        
        self._connected = False
        self._running = False
        
        if self.connection:
            await self.connection.close()
            self.connection = None
        
        print("🔌 Disconnected")
        
        if self.on_disconnect:
            self.on_disconnect()
        
        if self.auto_reconnect and self._reconnect_count < self.max_reconnects:
            print(f"Auto-reconnect in {self.reconnect_delay}s...")
            await asyncio.sleep(self.reconnect_delay)
            self._reconnect_count += 1
            self.stats['reconnects'] += 1
            asyncio.create_task(self.connect())
    
    def is_connected(self) -> bool:
        """Check if connected"""
        return self._connected and self.connection and not self.connection.is_closed()

class SyncClient:
    """TCP Client (Synchronous)"""
    
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.socket: Optional[socket.socket] = None
        self.connected = False
        self._request_counter = 0
        self._response_buffer = {}
        
        self.stats = {
            'messages_sent': 0,
            'messages_received': 0
        }
        
        self.on_connect: Optional[Callable] = None
        self.on_message: Optional[Callable[[bytes], Optional[bytes]]] = None
        self.on_disconnect: Optional[Callable] = None
        self.on_error: Optional[Callable] = None
    
    def connect(self, timeout: int = 5) -> bool:
        """Connect to server"""
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.settimeout(timeout)
            self.socket.connect((self.host, self.port))
            self.socket.settimeout(None)  # Remove timeout for blocking recv
            self.connected = True
            print(f"✅ Connected to {self.host}:{self.port}")
            
            if self.on_connect:
                self.on_connect()
            
            # Start listener thread
            listen_thread = threading.Thread(target=self._listen_loop, daemon=True)
            listen_thread.start()
            return True
            
        except Exception as e:
            print(f"❌ Connection failed: {e}")
            if self.on_error:
                self.on_error(e)
            return False
    
    def _listen_loop(self):
        """Listen for messages from server"""
        from .packet import PacketCodec
        
        buffer = bytearray()
        while self.connected:
            try:
                self.socket.settimeout(0.5)  # Short timeout for checking connection
                data = self.socket.recv(4096)
                if not data:
                    break
                
                buffer.extend(data)
                while True:
                    payload = PacketCodec.decode(buffer)
                    if payload is None:
                        break
                    
                    self.stats['messages_received'] += 1
                    
                    # Check if this is a response to a request
                    payload_str = payload.decode('utf-8', errors='ignore')
                    if payload_str.startswith("RESP:"):
                        # Extract request ID
                        parts = payload_str.split(":", 2)
                        if len(parts) >= 3:
                            req_id = parts[1]
                            actual_data = parts[2].encode('utf-8')
                            self._response_buffer[req_id] = actual_data
                            continue
                    
                    # Normal message
                    if self.on_message:
                        try:
                            response = self.on_message(payload)
                            if response:
                                self.send(response)
                        except Exception as e:
                            if self.on_error:
                                self.on_error(e)
                                
            except socket.timeout:
                continue
            except Exception as e:
                if self.connected and self.on_error:
                    self.on_error(e)
                break
        
        self.disconnect()
    
    def send(self, data: bytes) -> bool:
        """Send data to server"""
        from .packet import PacketCodec
        
        if not self.connected or not self.socket:
            return False
        
        try:
            packet = PacketCodec.encode(data)
            self.socket.send(packet)
            self.stats['messages_sent'] += 1
            return True
        except Exception as e:
            if self.on_error:
                self.on_error(e)
            self.disconnect()
            return False
    
    def send_and_receive(self, data: bytes, timeout: int = 5) -> Optional[bytes]:
        """Send data and wait for response"""
        from .packet import PacketCodec
        
        if not self.connected or not self.socket:
            return None
        
        # Generate request ID
        self._request_counter += 1
        req_id = str(self._request_counter)
        
        # Send with request ID
        data_with_id = f"REQ:{req_id}:".encode() + data
        
        try:
            # Send request
            packet = PacketCodec.encode(data_with_id)
            self.socket.send(packet)
            self.stats['messages_sent'] += 1
            
            # Wait for response
            start_time = time.time()
            while time.time() - start_time < timeout:
                if req_id in self._response_buffer:
                    response = self._response_buffer.pop(req_id)
                    return response
                time.sleep(0.01)
            
            # Timeout
            return None
            
        except Exception as e:
            if self.on_error:
                self.on_error(e)
            return None
    
    def disconnect(self):
        """Disconnect from server"""
        self.connected = False
        if self.socket:
            try:
                self.socket.close()
            except:
                pass
            self.socket = None
        
        if self.on_disconnect:
            self.on_disconnect()
        print("🔌 Disconnected")