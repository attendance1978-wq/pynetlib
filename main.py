"""
Main module - Convenience imports for pynetlib
Version 25.0
"""

from .server import Server, SyncServer
from .client import Client, SyncClient
from .udp import UdpServer, UdpClient
from .packet import Packet, PacketStatus, PacketCodec
from .pipeline import Pipeline, Handler, Context
from .connection import Connection
from .utils import (
    get_local_ip,
    get_public_ip,
    scan_port,
    get_hostname,
    create_packet,
    parse_packet,
    create_json_packet,
    is_port_available,
    find_available_port
)

def create_server(host: str = "0.0.0.0", port: int = 8888, sync: bool = False):
    """Create a server instance."""
    if sync:
        return SyncServer(host, port)
    return Server(host, port)

def create_client(host: str, port: int, sync: bool = False):
    """Create a client instance."""
    if sync:
        return SyncClient(host, port)
    return Client(host, port)

def create_udp_server(host: str = "0.0.0.0", port: int = 8888):
    """Create a UDP server instance."""
    return UdpServer(host, port)

def create_udp_client(host: str, port: int):
    """Create a UDP client instance."""
    return UdpClient(host, port)

def create_pipeline():
    """Create a new pipeline."""
    return Pipeline()

__version__ = "25.0"

__all__ = [
    'Server', 'SyncServer',
    'Client', 'SyncClient',
    'UdpServer', 'UdpClient',
    'Packet', 'PacketStatus', 'PacketCodec',
    'Pipeline', 'Handler', 'Context',
    'Connection',
    'create_server', 'create_client',
    'create_udp_server', 'create_udp_client',
    'create_pipeline',
    'get_local_ip', 'get_public_ip',
    'scan_port',
    'get_hostname',
    'create_packet', 'parse_packet',
    'create_json_packet',
    'is_port_available', 'find_available_port',
    '__version__'
]