# MINI_HDFS/common/utils.py
import json
import struct

def send_message(sock, message):
    """Sends a JSON message prefixed with its length."""
    try:
        data = json.dumps(message).encode('utf-8')
        # Prefix the message with its length (4 bytes, big-endian)
        length = struct.pack('>I', len(data))
        sock.sendall(length + data)
    except Exception as e:
        print(f"Error sending message: {e}")
        raise

def receive_message(sock):
    """Receives a JSON message prefixed with its length."""
    try:
        # Read message length (4 bytes)
        length_bytes = sock.recv(4)
        if not length_bytes:
            return None # Connection closed
        length = struct.unpack('>I', length_bytes)[0]
        
        # Read the message data
        data = sock.recv(length)
        while len(data) < length:
            data += sock.recv(length - len(data))
        
        return json.loads(data.decode('utf-8'))
    except Exception as e:
        print(f"Error receiving message: {e}")
        return None

def receive_file(sock, size):
    """Receives a binary file of a known size."""
    data = b''
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        if not chunk:
            raise Exception("File transfer interrupted.")
        data += chunk
    return data
