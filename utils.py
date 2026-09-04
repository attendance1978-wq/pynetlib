"""
Utils module - Helper functions and utilities
"""

import socket
import json
import time
from typing import Dict, Any, Tuple, Optional

from .packet import create_packet, parse_packet, create_json_packet

def get_local_ip() -> str:
    """Get local IP address"""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except:
        return "127.0.0.1"

def get_public_ip() -> str:
    """Get public IP address (requires internet)"""
    try:
        import urllib.request
        response = urllib.request.urlopen('https://api.ipify.org', timeout=5)
        return response.read().decode('utf-8')
    except:
        return "Unknown"

def scan_port(host: str, port: int, timeout: float = 1.0) -> bool:
    """Check if a port is open"""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(timeout)
        result = s.connect_ex((host, port))
        s.close()
        return result == 0
    except:
        return False

def scan_ports(host: str, ports: list, timeout: float = 1.0) -> Dict[int, bool]:
    """Scan multiple ports"""
    results = {}
    for port in ports:
        results[port] = scan_port(host, port, timeout)
    return results

def get_hostname() -> str:
    """Get local hostname"""
    try:
        return socket.gethostname()
    except:
        return "localhost"

def resolve_hostname(hostname: str) -> Optional[str]:
    """Resolve hostname to IP address"""
    try:
        return socket.gethostbyname(hostname)
    except:
        return None

def is_port_available(port: int) -> bool:
    """Check if a port is available for binding"""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(("0.0.0.0", port))
        s.close()
        return True
    except:
        return False

def find_available_port(start_port: int, max_attempts: int = 100) -> Optional[int]:
    """Find an available port starting from start_port"""
    for port in range(start_port, start_port + max_attempts):
        if is_port_available(port):
            return port
    return None

def create_response_packet(data: bytes, request_type: str = "response") -> bytes:
    """Create a response packet"""
    header = {
        "type": request_type,
        "timestamp": time.time(),
        "is_response": True
    }
    from .packet import Packet
    return Packet(header, data).to_bytes()

def parse_json_packet(data: bytes) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Parse a JSON packet"""
    from .packet import Packet
    packet = Packet.from_bytes(data)
    try:
        payload = json.loads(packet.payload.decode('utf-8'))
        return packet.header, payload
    except:
        return packet.header, {"error": "Invalid JSON payload"}