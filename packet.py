"""
Packet module - Handles packet encoding/decoding
"""

import json
import struct
import time
from typing import Dict, Any, Optional
from dataclasses import dataclass, field
from enum import Enum

class PacketStatus(Enum):
    OK = "ok"
    ERROR = "error"
    INCOMPLETE = "incomplete"
    TIMEOUT = "timeout"

@dataclass
class Packet:
    """Simple packet structure with header and payload"""
    header: Dict[str, Any] = field(default_factory=dict)
    payload: bytes = b""
    
    def to_bytes(self) -> bytes:
        """Convert packet to bytes for transmission"""
        try:
            header_json = json.dumps(self.header).encode('utf-8')
            header_len = len(header_json).to_bytes(4, 'big')
            return header_len + header_json + self.payload
        except Exception as e:
            raise ValueError(f"Packet serialization error: {e}")
    
    @classmethod
    def from_bytes(cls, data: bytes) -> 'Packet':
        """Create packet from received bytes"""
        try:
            if len(data) < 4:
                raise ValueError("Packet too small (missing header length)")
            
            header_len = int.from_bytes(data[:4], 'big')
            
            if len(data) < 4 + header_len:
                raise ValueError(f"Packet incomplete: expected {4 + header_len} bytes, got {len(data)}")
            
            header_data = data[4:4 + header_len]
            header = json.loads(header_data.decode('utf-8'))
            payload = data[4 + header_len:]
            
            return cls(header, payload)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in packet header: {e}")
        except Exception as e:
            raise ValueError(f"Packet deserialization error: {e}")
    
    def __str__(self) -> str:
        payload_preview = self.payload[:50] if self.payload else b""
        return f"Packet(header={self.header}, payload={payload_preview}...)"

class PacketCodec:
    """Packet codec for stream-based protocols"""
    HEADER_SIZE = 4
    
    @staticmethod
    def encode(data: bytes) -> bytes:
        """Encode data with length prefix"""
        return struct.pack('>I', len(data)) + data
    
    @staticmethod
    def decode(buffer: bytearray) -> Optional[bytes]:
        """Try to decode a packet from buffer"""
        if len(buffer) < PacketCodec.HEADER_SIZE:
            return None
        
        payload_len = struct.unpack('>I', buffer[:PacketCodec.HEADER_SIZE])[0]
        total_len = PacketCodec.HEADER_SIZE + payload_len
        
        if len(buffer) < total_len:
            return None
        
        payload = bytes(buffer[PacketCodec.HEADER_SIZE:total_len])
        del buffer[:total_len]
        return payload

def create_packet(data: bytes, packet_type: str = "data") -> bytes:
    """Helper to create a packet quickly"""
    header = {"type": packet_type, "timestamp": time.time()}
    return Packet(header, data).to_bytes()

def parse_packet(data: bytes) -> tuple:
    """Helper to parse a packet quickly"""
    packet = Packet.from_bytes(data)
    return packet.header, packet.payload

def create_json_packet(data: Dict[str, Any]) -> bytes:
    """Create a JSON packet"""
    return create_packet(json.dumps(data).encode('utf-8'), "json")