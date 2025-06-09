import asyncio
import websockets
import json
from datetime import datetime
from typing import Set
import logging

logger = logging.getLogger(__name__)

class WebSocketServer:
    def __init__(self):
        self.clients: Set[websockets.WebSocketServerProtocol] = set()
        self.server = None

    async def start(self):
        """Start the WebSocket server."""
        try:
            self.server = await websockets.serve(
                self.handle_client,
                "0.0.0.0",  # Bind to all interfaces
                8765,
                ping_interval=20,  # Send ping every 20 seconds
                ping_timeout=10,   # Wait 10 seconds for pong response
                close_timeout=10   # Wait 10 seconds for close handshake
            )
            logger.info("WebSocket server started on ws://0.0.0.0:8765")
        except Exception as e:
            logger.error(f"Failed to start WebSocket server: {e}")
            raise

    async def handle_client(self, websocket: websockets.WebSocketServerProtocol):
        """Handle a new client connection."""
        client_ip = websocket.remote_address
        logger.info(f'New client connection from {client_ip}')
        self.clients.add(websocket)
        try:
            async for message in websocket:
                logger.info(f'Received message from {client_ip}: {message}')
                # We don't expect any messages from clients, but handle them gracefully
                try:
                    # Try to parse as JSON for debugging
                    data = json.loads(message)
                    if data.get('type') == 'ping':
                        # Respond to ping messages
                        await websocket.send(json.dumps({'type': 'pong', 'timestamp': datetime.utcnow().timestamp()}))
                except json.JSONDecodeError:
                    # Not JSON, just log it
                    pass
        except websockets.exceptions.ConnectionClosed as e:
            logger.info(f"Client {client_ip} disconnected: {e}")
        except Exception as e:
            logger.error(f'Error with client {client_ip}: {e}')
        finally:
            # Safely remove client from set
            try:
                self.clients.discard(websocket)  # discard doesn't raise KeyError if not present
                logger.info(f"Client {client_ip} removed from active connections")
            except Exception as e:
                logger.error(f"Error removing client {client_ip}: {e}")

    async def broadcast(self, data: dict):
        """Broadcast data to all connected clients."""
        if not self.clients:
            return

        # Add timestamp to the data
        data['timestamp'] = datetime.utcnow().timestamp()
        
        # Convert to JSON
        message = json.dumps(data)
        
        # Create a copy of clients to avoid "Set changed size during iteration" error
        clients_copy = self.clients.copy()
        
        # Broadcast to all clients
        websockets_to_remove = set()
        for websocket in clients_copy:
            try:
                await websocket.send(message)
            except websockets.exceptions.ConnectionClosed:
                websockets_to_remove.add(websocket)
            except Exception as e:
                logger.error(f"Error broadcasting to client {id(websocket)}: {e}")
                websockets_to_remove.add(websocket)
        
        # Remove disconnected clients
        if websockets_to_remove:
            self.clients -= websockets_to_remove
            logger.info(f"Removed {len(websockets_to_remove)} disconnected clients")

    async def stop(self):
        """Stop the WebSocket server."""
        if self.server:
            self.server.close()
            await self.server.wait_closed()
            logger.info("WebSocket server stopped") 