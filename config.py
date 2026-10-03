# =========================================================
# MINI_HDFS/config.py
# Tailscale-based distributed configuration
# =========================================================

import os


# =========================================================
# NAMENODE CONFIGURATION
# =========================================================

# IP used by the NameNode server to LISTEN.
#
# 0.0.0.0 means:
# - localhost
# - Windows LAN interface
# - Tailscale interface
#
# The NameNode will therefore accept connections on all
# network interfaces.
NAMENODE_BIND_IP = '0.0.0.0'

# Windows machine Tailscale IP.
#
# Remote DataNodes use this IP to connect to the NameNode.
NAMENODE_IP = '100.104.93.92'

NAMENODE_PORT = 8000

# Address used by DataNodes/clients to connect to NameNode.
NAMENODE_ADDRESS = (NAMENODE_IP, NAMENODE_PORT)


# =========================================================
# DATANODE 1 CONFIGURATION
# =========================================================

# IP used by DataNode 1 server to LISTEN on Windows.
# 127.0.0.1 for local-only access.
DATANODE1_BIND_IP = '127.0.0.1'

# DataNode 1 is running on the same Windows machine.
DATANODE1_IP = '127.0.0.1'
DATANODE1_PORT = 8001

DATANODE1_ADDRESS = (
    DATANODE1_IP,
    DATANODE1_PORT
)


# =========================================================
# DATANODE 2 CONFIGURATION
# =========================================================

# IP used by DataNode 2 server to LISTEN on Windows.
# 127.0.0.1 for local-only access.
DATANODE2_BIND_IP = '127.0.0.1'

# DataNode 2 is also running on the same Windows machine.
DATANODE2_IP = '127.0.0.1'
DATANODE2_PORT = 8002

DATANODE2_ADDRESS = (
    DATANODE2_IP,
    DATANODE2_PORT
)


# =========================================================
# DATANODE 3 CONFIGURATION - AWS
# =========================================================

# IP used by DataNode 3 server to LISTEN on AWS.
# 0.0.0.0 means accept connections on all interfaces (Tailscale, etc).
DATANODE3_BIND_IP = '0.0.0.0'

# AWS EC2 DataNode 3 Tailscale IP.
# Remote NameNode/clients use this IP to connect to DataNode 3.
DATANODE3_IP = '100.114.20.84'
DATANODE3_PORT = 8003

DATANODE3_ADDRESS = (
    DATANODE3_IP,
    DATANODE3_PORT
)


# =========================================================
# ALL DATANODES
# =========================================================

DATANODE_ADDRESSES = [
    DATANODE1_ADDRESS,
    DATANODE2_ADDRESS,
    DATANODE3_ADDRESS,
]


# =========================================================
# DATANODE PORT -> IP MAPPING
# =========================================================

DATANODE_PORT_TO_IP = {
    DATANODE1_PORT: DATANODE1_IP,
    DATANODE2_PORT: DATANODE2_IP,
    DATANODE3_PORT: DATANODE3_IP,
}


# =========================================================
# CLIENT / DASHBOARD
# =========================================================

CLIENT_IP = '127.0.0.1'
CLIENT_PORT = 5000


# =========================================================
# HDFS CONSTANTS
# =========================================================

# Each file is divided into 2 MB chunks.
CHUNK_SIZE_MB = 2

CHUNK_SIZE_BYTES = (
    CHUNK_SIZE_MB * 1024 * 1024
)

# Each chunk should have 2 replicas.
REPLICATION_FACTOR = 2

# DataNode heartbeat interval.
HEARTBEAT_INTERVAL = 5

# DataNode heartbeat timeout (mark as DEAD after this many seconds without heartbeat).
HEARTBEAT_TIMEOUT = 15


# =========================================================
# FAULT TOLERANCE
# =========================================================

# Check for under-replicated chunks every 10 seconds.
REPLICATION_CHECK_INTERVAL = 10

# Maximum number of simultaneous replication tasks.
MAX_CONCURRENT_REPLICATIONS = 3


# =========================================================
# KAFKA EVENT LAYER
# =========================================================

KAFKA_ENABLED = os.getenv("KAFKA_ENABLED", "true").lower() == "true"
KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    f"{NAMENODE_IP}:9092",
)
KAFKA_HOST = os.getenv("KAFKA_HOST", NAMENODE_IP)
KAFKA_PORT = int(os.getenv("KAFKA_PORT", "9092"))
KAFKA_CONNECT_TIMEOUT_MS = int(
    os.getenv("KAFKA_CONNECT_TIMEOUT_MS", "10000")
)
KAFKA_RETRY_BACKOFF_SECONDS = float(
    os.getenv("KAFKA_RETRY_BACKOFF_SECONDS", "5")
)
KAFKA_CONSUMER_GROUP = os.getenv(
    "KAFKA_CONSUMER_GROUP",
    "mini-hdfs-dashboard",
)

KAFKA_TOPICS = (
    "datanode-events",
    "heartbeat-events",
    "chunk-events",
    "replication-events",
    "system-events",
)