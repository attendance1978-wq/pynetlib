"""
UDP module - UDP Server and Client implementation
"""

import socket
import threading
from typing import Optional, Dict, Any, Callable, Tuple

class UdpServer:
    """Simple UDP Server"""
    
    def __init__(self, host: str = '0.0.0.0', port: int = 8888):
        self.host = host
        self.port = port
        self.socket: Optional[socket.socket] = None
        self.running = False
        self._lock = threading.Lock()
        
        self.on_message: Optional[Callable[[bytes, Tuple[str, int]], Optional[bytes]]] = None
        self.on_error: Optional[Callable[[Exception], None]] = None
        
        self.stats = {
            'packets_received': 0,
            'packets_sent': 0,
            'errors': 0
        }
    
    def start(self) -> bool:
        """Start UDP server"""
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.socket.bind((self.host, self.port))
            self.running = True
            print(f"✅ UDP Server started on {self.host}:{self.port}")
            
            listen_thread = threading.Thread(target=self._listen_loop, daemon=True)
            listen_thread.start()
            return True
        except Exception as e:
            print(f"❌ Failed to start UDP server: {e}")
            return False
    
    def _listen_loop(self):
        """Listen for UDP packets"""
        while self.running:
            try:
                data, addr = self.socket.recvfrom(65536)
                self.stats['packets_received'] += 1
                
                if self.on_message:
                    try:
                        response = self.on_message(data, addr)
                        if response is not None:
                            self.send(response, addr)
                    except Exception as e:
                        if self.on_error:
                            self.on_error(e)
            except Exception as e:
                if self.running and self.on_error:
                    self.on_error(e)
    
    def send(self, data: bytes, addr: Tuple[str, int]) -> bool:
        """Send UDP packet to specific address"""
        if not self.socket or not self.running:
            return False
        try:
            self.socket.sendto(data, addr)
            self.stats['packets_sent'] += 1
            return True
        except Exception as e:
            if self.on_error:
                self.on_error(e)
            return False
    
    def broadcast(self, data: bytes, port: int) -> bool:
        """Broadcast UDP packet"""
        if not self.socket or not self.running:
            return False
        try:
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            self.socket.sendto(data, ('255.255.255.255', port))
            self.stats['packets_sent'] += 1
            return True
        except Exception as e:
            if self.on_error:
                self.on_error(e)
            return False
    
    def stop(self):
        """Stop UDP server"""
        self.running = False
        if self.socket:
            try:
                self.socket.close()
            except:
                pass
        print("🛑 UDP Server stopped")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get server statistics"""
        return {
            **self.stats,
            'running': self.running,
            'host': self.host,
            'port': self.port
        }

class UdpClient:
    """Simple UDP Client"""
    
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.socket: Optional[socket.socket] = None
        self.timeout = 5.0
        self.connected = False
        
        self.stats = {
            'packets_sent': 0,
            'packets_received': 0,
            'errors': 0
        }
    
    def connect(self) -> bool:
        """Setup UDP socket"""
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.socket.settimeout(self.timeout)
            self.connected = True
            print(f"✅ UDP Client ready for {self.host}:{self.port}")
            return True
        except Exception as e:
            print(f"❌ UDP client setup error: {e}")
            return False
    
    def send(self, data: bytes) -> bool:
        """Send UDP packet"""
        if not self.socket or not self.connected:
            return False
        try:
            self.socket.sendto(data, (self.host, self.port))
            self.stats['packets_sent'] += 1
            return True
        except Exception as e:
            self.stats['errors'] += 1
            return False
    
    def receive(self, timeout: float = None) -> Optional[bytes]:
        """Receive a UDP packet"""
        if not self.socket or not self.connected:
            return None
        
        try:
            if timeout is not None:
                self.socket.settimeout(timeout)
            
            data, _ = self.socket.recvfrom(65536)
            self.stats['packets_received'] += 1
            return data
            
        except socket.timeout:
            return None
        except Exception:
            return None
        finally:
            if timeout is not None:
                self.socket.settimeout(self.timeout)
    
    def send_and_receive(self, data: bytes, timeout: float = None) -> Optional[bytes]:
        """Send and wait for response"""
        if not self.socket or not self.connected:
            return None
        
        if self.send(data):
            return self.receive(timeout)
        return None
    
    def set_timeout(self, timeout: float):
        """Set socket timeout"""
        self.timeout = timeout
        if self.socket:
            try:
                self.socket.settimeout(timeout)
            except:
                pass
    
    def close(self):
        """Close UDP socket"""
        self.connected = False
        if self.socket:
            try:
                self.socket.close()
            except:
                pass
            self.socket = None
        print("🔌 UDP Client closed")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get client statistics"""
        return {
            **self.stats,
            'host': self.host,
            'port': self.port,
            'connected': self.connected
        }