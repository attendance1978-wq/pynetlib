"""
Pipeline module - Handler chain for processing network data
"""

from typing import Optional, Dict, Any, Callable, List, Tuple

class Context:
    """Context object passed through the pipeline"""
    
    def __init__(self, pipeline: 'Pipeline', name: str):
        self.pipeline = pipeline
        self.name = name
        self.attrs: Dict[str, Any] = {}
        self._connection = None
    
    def get_pipeline(self) -> 'Pipeline':
        return self.pipeline
    
    def get_attr(self, key: str, default=None) -> Any:
        return self.attrs.get(key, default)
    
    def set_attr(self, key: str, value: Any):
        self.attrs[key] = value
    
    def has_attr(self, key: str) -> bool:
        return key in self.attrs
    
    def set_connection(self, connection):
        self._connection = connection
    
    def get_connection(self):
        return self._connection

class Handler:
    """Interface for pipeline handlers"""
    
    async def on_connect(self, ctx: Context):
        """Called when a new connection is established"""
        pass
    
    async def on_read(self, ctx: Context, data: bytes):
        """Called when data is received"""
        pass
    
    async def on_write(self, ctx: Context, data: bytes) -> bytes:
        """Called before writing data"""
        return data
    
    async def on_disconnect(self, ctx: Context):
        """Called when the connection is closed"""
        pass
    
    async def on_error(self, ctx: Context, error: Exception):
        """Called when an error occurs"""
        pass

class Pipeline:
    """A chain of handlers that process data in order"""
    
    def __init__(self):
        self.handlers: List[Tuple[str, Handler]] = []
        self.contexts: Dict[str, Context] = {}
        self._connection = None
    
    def add_last(self, name: str, handler: Handler) -> 'Pipeline':
        """Add a handler to the end of the pipeline"""
        self.handlers.append((name, handler))
        return self
    
    def add_first(self, name: str, handler: Handler) -> 'Pipeline':
        """Add a handler to the beginning of the pipeline"""
        self.handlers.insert(0, (name, handler))
        return self
    
    def remove(self, name: str) -> 'Pipeline':
        """Remove a handler from the pipeline"""
        for i, (h_name, _) in enumerate(self.handlers):
            if h_name == name:
                self.handlers.pop(i)
                self.contexts.pop(name, None)
                return self
        return self
    
    def get_context(self, name: str) -> Optional[Context]:
        """Get context for a handler by name"""
        return self.contexts.get(name)
    
    def get_handler(self, name: str) -> Optional[Handler]:
        """Get handler by name"""
        for h_name, handler in self.handlers:
            if h_name == name:
                return handler
        return None
    
    def _create_contexts(self):
        """Create contexts for all handlers"""
        self.contexts = {}
        for name, _ in self.handlers:
            ctx = Context(self, name)
            ctx.set_connection(self._connection)
            self.contexts[name] = ctx
    
    def set_connection(self, connection):
        """Set the connection for all contexts"""
        self._connection = connection
        for ctx in self.contexts.values():
            ctx.set_connection(connection)
    
    async def fire_connect(self):
        """Fire connect event through the pipeline"""
        self._create_contexts()
        for name, handler in self.handlers:
            try:
                ctx = self.contexts[name]
                await handler.on_connect(ctx)
            except Exception as e:
                await self.fire_error(e)
                break
    
    async def fire_read(self, data: bytes) -> bytes:
        """Fire read event through the pipeline"""
        current = data
        for name, handler in self.handlers:
            try:
                ctx = self.contexts[name]
                await handler.on_read(ctx, current)
            except Exception as e:
                await self.fire_error(e)
                return current
        return current
    
    async def fire_write(self, data: bytes) -> bytes:
        """Fire write event through the pipeline (reverse order)"""
        current = data
        for name, handler in reversed(self.handlers):
            try:
                ctx = self.contexts[name]
                current = await handler.on_write(ctx, current)
            except Exception as e:
                await self.fire_error(e)
                return current
        return current
    
    async def fire_disconnect(self):
        """Fire disconnect event through the pipeline"""
        for name, handler in self.handlers:
            try:
                ctx = self.contexts[name]
                await handler.on_disconnect(ctx)
            except Exception as e:
                await self.fire_error(e)
                break
    
    async def fire_error(self, error: Exception):
        """Fire error event through the pipeline"""
        for name, handler in self.handlers:
            try:
                ctx = self.contexts[name]
                await handler.on_error(ctx, error)
            except Exception:
                pass
    
    def get_handlers(self) -> List[str]:
        """Get list of handler names"""
        return [name for name, _ in self.handlers]