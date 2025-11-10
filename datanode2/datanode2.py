# MINI_HDFS/datanode2/datanode2.py
import os
import sys
import socket
import threading
import time
import json

# --- IMPORT FIX ---
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from config import (
    NAMENODE_ADDRESS, DATANODE2_ADDRESS, HEARTBEAT_INTERVAL, CHUNK_SIZE_BYTES
)
from common.utils import send_message, receive_message, receive_file 

# --- Datanode State ---
DATANODE_ID = "DN-5003"
STORAGE_DIR = os.path.join(os.path.dirname(__file__), 'storage')
os.makedirs(STORAGE_DIR, exist_ok=True)


def send_heartbeat():
    """Sends periodic status to the Namenode."""
    while True:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                # Connect to the Namenode's ZeroTier IP
                s.connect(NAMENODE_ADDRESS) 
                
                send_message(s, {
                    'type': 'heartbeat',
                    'datanode_id': DATANODE_ID,
                    'port': DATANODE2_ADDRESS[1] # Report its own port
                })
        except Exception as e:
            # If Namenode is down, the Datanode just logs/passes and retries
            pass
        
        time.sleep(HEARTBEAT_INTERVAL)


def handle_chunk_operation(conn):
    """Handles chunk storage, retrieval, and COPY requests."""
    try:
        request = receive_message(conn)
        if not request:
            return

        request_type = request.get('type')
        chunk_id = request.get('chunk_id')
        chunk_path = os.path.join(STORAGE_DIR, chunk_id)

        if request_type == 'store_chunk':
            size = request.get('size')
            chunk_data = receive_file(conn, size)
            
            with open(chunk_path, 'wb') as f:
                f.write(chunk_data)
            
            print(f"Stored chunk {chunk_id} with size {size} bytes.")
            send_message(conn, {'status': 'success', 'message': f'Stored {chunk_id}'})

        elif request_type == 'get_chunk':
            if os.path.exists(chunk_path):
                file_size = os.path.getsize(chunk_path)
                # Send size first (4 bytes)
                conn.sendall(file_size.to_bytes(4, byteorder='big')) 
                
                with open(chunk_path, 'rb') as f:
                    conn.sendall(f.read())
                print(f"Sent chunk {chunk_id}.")
            else:
                conn.sendall(b'\x00\x00\x00\x00') # Send 0 size
                print(f"Chunk {chunk_id} not found.")

        # ==================================================
        # NEW: Handle Re-replication Request (COPY CHUNK)
        # ==================================================
        elif request_type == 'copy_chunk':
            destination_ip = request.get('destination_ip')
            destination_port = request.get('destination_port')
            destination_addr = (destination_ip, destination_port)
            
            chunk_path = os.path.join(STORAGE_DIR, chunk_id)
            
            if os.path.exists(chunk_path):
                # 1. Read the chunk data
                with open(chunk_path, 'rb') as f:
                    chunk_data = f.read()
                file_size = len(chunk_data)
                
                # 2. Connect directly to the destination Datanode
                print(f"Copying {chunk_id} to {destination_ip}:{destination_port}")
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as dest_sock:
                    dest_sock.connect(destination_addr)
                    
                    # 3. Instruct destination to store the chunk
                    send_message(dest_sock, {
                        'type': 'store_chunk',
                        'chunk_id': chunk_id,
                        'size': file_size
                    })
                    
                    # 4. Send the data payload
                    dest_sock.sendall(chunk_data)
                    
                    # 5. Send success ACK back to the initiating Namenode connection
                    send_message(conn, {'status': 'success', 'message': f'Copied {chunk_id} successfully'})
            else:
                send_message(conn, {'status': 'error', 'message': f'Chunk {chunk_id} not found locally for copy'})


    except Exception as e:
        print(f"Datanode connection error: {e}")
    finally:
        conn.close()


def start_datanode():
    """Main function to start the Datanode listener."""
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # Bind to the Datanode 2's ZeroTier IP
    server_socket.bind(DATANODE2_ADDRESS)
    server_socket.listen(10)
    print(f"Datanode 2 ({DATANODE_ID}) listening on {DATANODE2_ADDRESS[0]}:{DATANODE2_ADDRESS[1]}")

    # Start the heartbeat sender thread
    threading.Thread(target=send_heartbeat, daemon=True).start()

    while True:
        conn, addr = server_socket.accept()
        threading.Thread(target=handle_chunk_operation, args=(conn,), daemon=True).start()

if __name__ == '__main__':
    start_datanode()
