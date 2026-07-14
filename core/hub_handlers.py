"""Async WebSocket handlers for hub mode"""

import asyncio
import logging
import json
from fastapi import WebSocket

logger = logging.getLogger(__name__)

# Heartbeat fallback so the event-driven loop still re-checks hub.running when nodes are idle
HEARTBEAT_INTERVAL = 5.0

# Global WebSocket connections
websocket_connections = set()

def register_hub_handlers(app, hub):
    """Register FastAPI WebSocket handlers for hub mode"""
    
    @app.websocket("/socket.io/")
    async def websocket_endpoint(websocket: WebSocket):
        await websocket.accept()
        websocket_connections.add(websocket)
        logger.debug('Dashboard client connected')
        
        if not hub.running:
            hub.running = True
            asyncio.create_task(hub_loop(hub, websocket_connections))
        
        # Start node connections if not already started
        if not hub._connection_started:
            hub._connection_started = True
            asyncio.create_task(hub._connect_all_nodes())
        
        try:
            # Keep connection alive
            while True:
                await websocket.receive_text()
        except Exception as e:
            logger.debug(f'Dashboard client disconnected: {e}')
        finally:
            websocket_connections.discard(websocket)
            # Pause aggregation when nobody is watching to avoid idle CPU usage.
            if not websocket_connections and hub.running:
                hub.running = False
                logger.info("No active clients — pausing hub loop")


async def hub_loop(hub, connections):
    """Async background loop that broadcasts aggregated cluster data on each node update"""
    logger.info("Hub monitoring loop started")

    while hub.running:
        try:
            cluster_data = await hub.get_cluster_data()

            # Send to all connected clients (copy to avoid mutation during iteration)
            if connections:
                disconnected = set()
                for websocket in list(connections):
                    try:
                        await websocket.send_text(json.dumps(cluster_data))
                    except Exception:
                        disconnected.add(websocket)

                # Remove disconnected clients
                connections -= disconnected

        except Exception as e:
            logger.error(f"Error in hub loop: {e}")

        # Wake on the next node update; timeout is just the heartbeat
        await hub.wait_for_update(timeout=HEARTBEAT_INTERVAL)

