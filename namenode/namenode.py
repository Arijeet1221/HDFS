# MINI_HDFS/namenode/namenode.py
import os
import sys
import socket
import threading
import json
import time
import random

# =========================================================
# FIX: Add the parent directory (MINI_HDFS/) to the Python path
# =========================================================
# This ensures modules like 'config' and 'common.utils' are found.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import from the project root
from config import (
    NAMENODE_ADDRESS, DATANODE_ADDRESSES, HEARTBEAT_INTERVAL, CHUNK_SIZE_BYTES,
    DATANODE_PORT_TO_IP
)
from common.utils import send_message, receive_message

# --- Global Namenode State ---
# metadata: {filename: [{'chunk_id': ..., 'datanodes': [addr1, addr2]}, ...]}
file_metadata = {}
# health: {(ip, port): {'status': 'ALIVE'/'DEAD', 'last_check': timestamp}}
datanode_health = {addr: {'status': 'DEAD', 'last_check': 0} for addr in DATANODE_ADDRESSES}
# Map for simple Datanode identification in the dashboard
DATANODE_MAP = {
    DATANODE_ADDRESSES[0]: 'DN-5002', # Athrav
    DATANODE_ADDRESSES[1]: 'DN-5003', # Arjith
}
metadata_lock = threading.Lock()

def handle_client_connection(conn):
    """Handles requests from the Client (Upload, Download, Status)."""
    try:
        request = receive_message(conn)
        if not request: return

        request_type = request.get('type')
        filename = request.get('filename')

        with metadata_lock:
            if request_type == 'upload_file':
                # Identify currently ALIVE Datanodes
                available_nodes = [addr for addr, health in datanode_health.items() if health['status'] == 'ALIVE']
                
                if len(available_nodes) < 2:
                    send_message(conn, {'status': 'error', 'message': 'Not enough Datanodes alive for replication (requires 2).'})
                    return

                response_chunks = []
                for chunk_id in request.get('chunk_ids', []):
                    # Randomly select 2 available Datanodes for replication
                    selected_datanodes = random.sample(available_nodes, 2)
                    
                    response_chunks.append({
                        'chunk_id': chunk_id,
                        # Namenode tells the client the ports to connect to
                        'datanodes': [addr[1] for addr in selected_datanodes]
                    })
                
                # Update metadata for the file (Store full (IP, Port) address)
                file_metadata[filename] = [
                    {'chunk_id': rc['chunk_id'], 'datanodes': [
                        (DATANODE_PORT_TO_IP[rc_port], rc_port) for rc_port in rc['datanodes']
                    ]}
                    for rc in response_chunks
                ]
                print(f"Metadata stored for {filename} ({len(response_chunks)} chunks).")
                send_message(conn, {'status': 'success', 'chunks': response_chunks})

            elif request_type == 'download_file':
                if filename not in file_metadata:
                    send_message(conn, {'status': 'error', 'message': 'File not found.'})
                    return
                
                # Send the list of chunks and their Datanode locations (ports)
                chunk_map = [
                    {
                        'chunk_id': c['chunk_id'], 
                        # Return PORTS for the Client to use for socket connection
                        'datanodes': [addr[1] for addr in c['datanodes']] 
                    } for c in file_metadata[filename]
                ]
                send_message(conn, {'status': 'success', 'chunks': chunk_map})

            elif request_type == 'list_files':
                send_message(conn, {'status': 'success', 'files': list(file_metadata.keys())})

            elif request_type == 'get_file_info':
                if filename not in file_metadata:
                    send_message(conn, {'status': 'error', 'message': 'File not found.'})
                    return
                
                # Prepare detailed status for the dashboard
                detailed_chunks = []
                for chunk in file_metadata[filename]:
                    chunk_info = {
                        'chunk_id': chunk['chunk_id'],
                        'datanodes': [DATANODE_MAP[addr] for addr in chunk['datanodes']],
                        'datanode_status': {
                            DATANODE_MAP[addr]: datanode_health[addr]['status'].lower()
                            for addr in chunk['datanodes']
                        }
                    }
                    detailed_chunks.append(chunk_info)
                
                send_message(conn, {'status': 'success', 'chunks': detailed_chunks})

    except Exception as e:
        print(f"Namenode client connection error: {e}")
    finally:
        conn.close()

def handle_datanode_connection(conn):
    """Handles heartbeats from Datanodes."""
    try:
        request = receive_message(conn)
        if not request: return

        if request.get('type') == 'heartbeat':
            dn_ip = conn.getpeername()[0]
            dn_port = request.get('port')
            dn_addr = (dn_ip, dn_port)
            
            # Check if address is a known datanode before updating
            if dn_addr in datanode_health:
                with metadata_lock:
                    datanode_health[dn_addr]['status'] = 'ALIVE'
                    datanode_health[dn_addr]['last_check'] = time.time()
                print(f"Heartbeat received from Datanode {DATANODE_MAP.get(dn_addr, 'Unknown')}")
            
    except Exception as e:
        # print(f"Error handling Datanode message: {e}")
        pass
    finally:
        conn.close()


def check_datanode_health():
    """Periodically checks Datanode health based on last received heartbeat timestamp."""
    while True:
        time.sleep(HEARTBEAT_INTERVAL * 2) # Check every 10 seconds
        timeout = HEARTBEAT_INTERVAL * 3 # Mark dead after 15 seconds
        current_time = time.time()
        
        with metadata_lock:
            for addr, health in datanode_health.items():
                if health['status'] == 'ALIVE' and (current_time - health['last_check']) > timeout:
                    health['status'] = 'DEAD'
                    print(f"Datanode {DATANODE_MAP.get(addr, 'Unknown')} ({addr}) timed out. Marking as DEAD.")


def start_namenode():
    """Main function to start the Namenode listener."""
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) # Allows quick restart
    # FIX: Bind to the Namenode's ZeroTier IP
    server_socket.bind(NAMENODE_ADDRESS) 
    server_socket.listen(10)
    print(f"Namenode listening on {NAMENODE_ADDRESS[0]}:{NAMENODE_ADDRESS[1]}")

    # Start health checker thread
    threading.Thread(target=check_datanode_health, daemon=True).start()

    while True:
        conn, addr = server_socket.accept()
        # Check if the connection source is a known Datanode IP
        if addr[0] in [a[0] for a in DATANODE_ADDRESSES]:
             threading.Thread(target=handle_datanode_connection, args=(conn,), daemon=True).start()
        else:
             threading.Thread(target=handle_client_connection, args=(conn,), daemon=True).start()

if __name__ == '__main__':
    start_namenode()
