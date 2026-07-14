"""Async Hub mode - aggregates data from multiple nodes"""

import asyncio
import logging
import json
import websockets
from datetime import datetime
from . import config

logger = logging.getLogger(__name__)


class Hub:
    """Aggregates GPU data from multiple nodes"""
    
    def __init__(self, node_urls):
        self.node_urls = node_urls
        self.nodes = {}  # node_name -> {client, data, status, last_update}
        self.url_to_node = {}  # url -> node_name mapping
        self.running = False
        self._connection_started = False
        self._update_event = None  # created lazily on the running loop

        # Initialize nodes as offline
        for url in node_urls:
            self.nodes[url] = {
                'url': url,
                'websocket': None,
                'data': None,
                'status': 'offline',
                'last_update': None
            }
            self.url_to_node[url] = url

    def _get_update_event(self):
        """Lazily create the update event so it binds to the running loop."""
        if self._update_event is None:
            self._update_event = asyncio.Event()
        return self._update_event

    def signal_update(self):
        """Wake the broadcast loop — a node's data or status changed."""
        self._get_update_event().set()

    async def wait_for_update(self, timeout):
        """Block until a node delivers new data, or until timeout (heartbeat)."""
        event = self._get_update_event()
        try:
            await asyncio.wait_for(event.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            pass
        finally:
            event.clear()

    async def _connect_all_nodes(self):
        """Connect to all nodes in background with retries"""
        # Wait a bit for Docker network to be ready
        await asyncio.sleep(2)
        
        # Connect to all nodes concurrently
        tasks = [self._connect_node_with_retry(url) for url in self.node_urls]
        await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _connect_node_with_retry(self, url):
        """Connect to a node with retry logic"""
        max_retries = 5
        retry_delay = 2
        
        for attempt in range(max_retries):
            try:
                await self._connect_node(url)
                return  # Success
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(f'Connection attempt {attempt + 1}/{max_retries} failed for {url}: {str(e)}, retrying in {retry_delay}s...')
                    await asyncio.sleep(retry_delay)
                else:
                    logger.error(f'Failed to connect to node {url} after {max_retries} attempts: {str(e)}')
    
    async def _connect_node(self, url):
        """Connect to a node using native WebSocket"""
        while self.running:
            try:
                # Convert HTTP URL to WebSocket URL
                ws_url = url.replace('http://', 'ws://').replace('https://', 'wss://') + '/socket.io/'
                
                logger.info(f'Connecting to node WebSocket: {ws_url}')
                
                async with websockets.connect(ws_url) as websocket:
                    logger.info(f'Connected to node: {url}')
                    
                    # Mark node as online
                    node_name = self.url_to_node.get(url, url)
                    self.nodes[node_name] = {
                        'url': url,
                        'websocket': websocket,
                        'data': None,
                        'status': 'online',
                        'last_update': datetime.now().isoformat()
                    }
                    self.signal_update()

                    # Listen for data from the node
                    async for message in websocket:
                        try:
                            data = json.loads(message)
                            
                            # Extract node name from data or use URL as fallback
                            node_name = data.get('node_name', url)
                            
                            # Update URL to node mapping
                            self.url_to_node[url] = node_name
                            
                            # Update node entry with received data
                            self.nodes[node_name] = {
                                'url': url,
                                'websocket': websocket,
                                'data': data,
                                'status': 'online',
                                'last_update': datetime.now().isoformat()
                            }
                            self.signal_update()

                        except json.JSONDecodeError as e:
                            logger.error(f'Failed to parse message from {url}: {e}')
                        except Exception as e:
                            logger.error(f'Error processing message from {url}: {e}')
                            
            except websockets.exceptions.ConnectionClosed:
                logger.warning(f'WebSocket connection closed for node: {url}')
                # Mark node as offline
                node_name = self.url_to_node.get(url, url)
                if node_name in self.nodes:
                    self.nodes[node_name]['status'] = 'offline'
                    logger.info(f'Marked node {node_name} as offline')
                    self.signal_update()
            except Exception as e:
                logger.error(f'Failed to connect to node {url}: {e}')
                # Mark node as offline
                node_name = self.url_to_node.get(url, url)
                if node_name in self.nodes:
                    self.nodes[node_name]['status'] = 'offline'
                    logger.info(f'Marked node {node_name} as offline')
                    self.signal_update()
            
            # Wait before retrying connection
            if self.running:
                await asyncio.sleep(5)
    
    async def get_cluster_data(self):
        """Get aggregated data from all nodes"""
        nodes = {}
        total_gpus = 0
        online_nodes = 0
        
        for node_name, node_info in self.nodes.items():
            if node_info['status'] == 'online' and node_info['data']:
                nodes[node_name] = {
                    'status': 'online',
                    'gpus': node_info['data'].get('gpus', {}),
                    'processes': node_info['data'].get('processes', []),
                    'system': node_info['data'].get('system', {}),
                    'last_update': node_info['last_update']
                }
                total_gpus += len(node_info['data'].get('gpus', {}))
                online_nodes += 1
            else:
                nodes[node_name] = {
                    'status': 'offline',
                    'gpus': {},
                    'processes': [],
                    'system': {},
                    'last_update': node_info.get('last_update')
                }
        
        return {
            'mode': 'hub',
            'nodes': nodes,
            'cluster_stats': {
                'total_nodes': len(self.nodes),
                'online_nodes': online_nodes,
                'total_gpus': total_gpus
            }
        }
    
    async def shutdown(self):
        """Disconnect from all nodes"""
        self.running = False
        for node_info in self.nodes.values():
            if node_info.get('websocket'):
                try:
                    await node_info['websocket'].close()
                except:
                    pass

