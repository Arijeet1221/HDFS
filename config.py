# =========================================================
# MINI_HDFS/config.py: CENTRAL NETWORK CONFIGURATION
# 
# FINAL VERIFIED ZEROTIER IPs (Based on 68bea79acfe3b871 network):
# Namenode (Ramesh): 172.22.18.164
# Datanode 1 (Athrav): 172.22.89.80
# Datanode 2 (Arjith): 172.22.120.127
# Client (Ranjith): 172.22.73.252
# =========================================================

# --- Namenode Configuration (Ramesh) ---
NAMENODE_IP = '172.22.18.164'  # Ramesh's ZeroTier IP
NAMENODE_PORT = 8000
NAMENODE_ADDRESS = (NAMENODE_IP, NAMENODE_PORT)

# --- Datanode 1 Configuration (Athrav) ---
DATANODE1_IP = '172.22.89.80'
DATANODE1_PORT = 8001
DATANODE1_ADDRESS = (DATANODE1_IP, DATANODE1_PORT)

# --- Datanode 2 Configuration (Arjith) ---
DATANODE2_IP = '172.22.120.127'
DATANODE2_PORT = 8002
DATANODE2_ADDRESS = (DATANODE2_IP, DATANODE2_PORT)

# List of all Datanode addresses for the Namenode to manage
DATANODE_ADDRESSES = [
    DATANODE1_ADDRESS,
    DATANODE2_ADDRESS,
]

# Map Datanode ports to IPs for Client utility (since Namenode returns ports)
DATANODE_PORT_TO_IP = {
    DATANODE1_PORT: DATANODE1_IP,
    DATANODE2_PORT: DATANODE2_IP,
}

# --- Client/Dashboard Configuration (Ranjith) ---
CLIENT_IP = '172.22.73.252'   # Ranjith's ZeroTier IP
CLIENT_PORT = 5000 

# --- HDFS Constants ---
CHUNK_SIZE_MB = 2 
CHUNK_SIZE_BYTES = CHUNK_SIZE_MB * 1024 * 1024
REPLICATION_FACTOR = 2 
HEARTBEAT_INTERVAL = 5 # seconds
