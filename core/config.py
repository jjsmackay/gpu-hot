"""
Configuration settings for GPU Hot
"""

import os
import socket
import logging

logger = logging.getLogger(__name__)


def _positive_float_env(name, default):
    """Read a float env var, falling back to default if unset, non-numeric, or <= 0."""
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = float(raw)
    except ValueError:
        logger.warning(f"{name}={raw!r} is not a number; using default {default}s")
        return default
    if value <= 0:
        logger.warning(f"{name}={value} must be > 0; using default {default}s")
        return default
    return value

# Server Configuration
SECRET_KEY = 'gpu_hot_secret'
HOST = '0.0.0.0'
PORT = 1312
DEBUG = False

# Monitoring Configuration
# Both intervals are overridable via env vars (seconds, float).
UPDATE_INTERVAL = _positive_float_env('UPDATE_INTERVAL', 0.5)         # NVML polling interval
NVIDIA_SMI_INTERVAL = _positive_float_env('NVIDIA_SMI_INTERVAL', 2.0) # nvidia-smi fallback interval

# GPU Monitoring Mode
# Can be set via environment variable: NVIDIA_SMI=true
NVIDIA_SMI = os.getenv('NVIDIA_SMI', 'false').lower() == 'true'

# Multi-Node Configuration
# MODE: default (single node monitoring), hub (aggregate multiple nodes)
MODE = os.getenv('GPU_HOT_MODE', 'default')
NODE_NAME = os.getenv('NODE_NAME', socket.gethostname())
# NODE_URLS: comma-separated URLs for hub mode (e.g., http://node1:1312,http://node2:1312)
NODE_URLS = [url.strip() for url in os.getenv('NODE_URLS', '').split(',') if url.strip()]

