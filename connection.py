"""
Connection module - Handles individual network connections
"""

import asyncio
import time
from typing import Optional, Dict, Any

from .packet import PacketCodec
from .pipeline import Pipeline

class Connection:
    """Represents a single network connection"""
    
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, 
                 pipeline: Optional[Pipeline] = None):
        self.reader = reader
        self.writer = writer
        self.pipeline = pipeline or Pipeline()
        self.pipeline.set_connection(self)
        
        self._buffer = bytearray()
        self._closed = False
        self._write_lock = asyncio.Lock()
        
        self.remote_addr = writer.get_extra_info('peername')
        self.local_addr = writer.get_extra_info('sockname')
        self.connected_at = time.time()
        self.last_active = time.time()
        self.stats = {
            'bytes_received': 0,
            'bytes_sent': 0,
            'messages_received': 0,
            'messages_sent': 0
        }
        self.attrs: Dict[str, Any] = {}
    
    async def start(self):
        """Start the connection read loop"""
        await self.pipeline.fire_connect()
        
        try:
            while not self._closed:
                chunk = await self.reader.read(4096)
                if not chunk:
                    break
                
                self._buffer.extend(chunk)
                self.stats['bytes_received'] += len(chunk)
                self.last_active = time.time()
                
                while True:
                    payload = PacketCodec.decode(self._buffer)
                    if payload is None:
                        break
                    self.stats['messages_received'] += 1
                    await self.pipeline.fire_read(payload)
                    
        except asyncio.CancelledError:
            pass
        except Exception as e:
            await self.pipeline.fire_error(e)
        finally:
            await self.close()
    
    async def write(self, data: bytes) -> bool:
        """Write data to the connection"""
        if self._closed:
            return False
        
        async with self._write_lock:
            try:
                processed = await self.pipeline.fire_write(data)
                packet = PacketCodec.encode(processed)
                self.writer.write(packet)
                await self.writer.drain()
                self.stats['bytes_sent'] += len(packet)
                self.stats['messages_sent'] += 1
                self.last_active = time.time()
                return True
            except Exception as e:
                await self.pipeline.fire_error(e)
                return False
    
    async def write_raw(self, data: bytes) -> bool:
        """Write raw data without encoding"""
        if self._closed:
            return False
        
        try:
            self.writer.write(data)
            await self.writer.drain()
            self.stats['bytes_sent'] += len(data)
            self.last_active = time.time()
            return True
        except Exception as e:
            await self.pipeline.fire_error(e)
            return False
    
    async def close(self):
        """Close the connection gracefully"""
        if self._closed:
            return
        
        self._closed = True
        await self.pipeline.fire_disconnect()
        
        try:
            self.writer.close()
            await self.writer.wait_closed()
        except:
            pass
    
    def is_closed(self) -> bool:
        """Check if the connection is closed"""
        return self._closed
    
    def get_stats(self) -> Dict[str, Any]:
        """Get connection statistics"""
        return {
            **self.stats,
            'remote_addr': self.remote_addr,
            'local_addr': self.local_addr,
            'connected_at': self.connected_at,
            'last_active': self.last_active,
            'uptime': time.time() - self.connected_at
        }
    
    def get_attr(self, key: str, default=None) -> Any:
        """Get a connection attribute"""
        return self.attrs.get(key, default)
    
    def set_attr(self, key: str, value: Any):
        """Set a connection attribute"""
        self.attrs[key] = value