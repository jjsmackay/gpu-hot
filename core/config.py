"""
Configuration settings for GPU Hot
"""

import os
import socket

# Server Configuration
SECRET_KEY = 'gpu_hot_secret'
HOST = '0.0.0.0'
PORT = 1312
DEBUG = False

# Monitoring Configuration
# GPU Monitoring Mode
# Can be set via environment variable: NVIDIA_SMI=true
NVIDIA_SMI = os.getenv('NVIDIA_SMI', 'false').lower() == 'true'

# Monitoring Configuration
# Defaults to 2.0s for nvidia-smi (slower to reduce overhead), 0.5s for NVML
_default_interval = '2.0' if NVIDIA_SMI else '0.5'
UPDATE_INTERVAL = float(os.getenv('UPDATE_INTERVAL', _default_interval))

# Multi-Node Configuration
# MODE: default (single node monitoring), hub (aggregate multiple nodes)
MODE = os.getenv('GPU_HOT_MODE', 'default')
NODE_NAME = os.getenv('NODE_NAME', socket.gethostname())
# NODE_URLS: comma-separated URLs for hub mode (e.g., http://node1:1312,http://node2:1312)
NODE_URLS = [url.strip() for url in os.getenv('NODE_URLS', '').split(',') if url.strip()]

