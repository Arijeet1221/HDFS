# MINI_HDFS/client/client.py
import os
import sys
import socket
import threading
import uuid
import hashlib
import requests # Used for making requests to the Namenode API

from flask import Flask, render_template, request, jsonify, send_file
# --- CORRECTED IMPORTS ---
# Use single-dot imports for packages (config, common) available in the root
# when executing with 'python -m client.client' from the MINI_HDFS directory.
from config import ( 
    NAMENODE_IP, NAMENODE_PORT, DATANODE_PORT_TO_IP, 
    CLIENT_IP, CLIENT_PORT, CHUNK_SIZE_BYTES
)
from common.utils import send_message, receive_message, receive_file 


app = Flask(__name__, template_folder='templates')

# Set absolute path for upload folder
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# FIX: Adjusted path to use the root uploads folder
UPLOAD_FOLDER = os.path.join(os.path.dirname(BASE_DIR), 'uploads') 
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024 

# Ensure upload folder exists
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)


class HDFSClient:
    # FIX: Use NAMENODE_IP defined in config
    def __init__(self, namenode_host=NAMENODE_IP): 
        self.namenode_host = namenode_host
        self.namenode_port = NAMENODE_PORT
    
    def _generate_chunk_id(self, filename, chunk_index):
        """Generate unique chunk ID"""
        return f"{filename}_{chunk_index}_{uuid.uuid4().hex[:8]}"
    
    def upload_file(self, filepath):
        """Upload a file to HDFS"""
        try:
            # Read and chunk the file
            with open(filepath, 'rb') as f:
                chunks = []
                chunk_index = 0
                
                while True:
                    # FIX: Use CHUNK_SIZE_BYTES from config
                    chunk_data = f.read(CHUNK_SIZE_BYTES) 
                    if not chunk_data:
                        break
                    
                    chunk_id = self._generate_chunk_id(os.path.basename(filepath), chunk_index)
                    chunks.append({
                        'chunk_id': chunk_id,
                        'data': chunk_data
                    })
                    chunk_index += 1
            
            if len(chunks) == 0:
                return {'status': 'error', 'message': 'File is empty'}
            
            # 1. Get chunk placement from namenode
            namenode_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # FIX: Connect to the Namenode's ZeroTier IP
            namenode_sock.connect((self.namenode_host, self.namenode_port)) 
            
            send_message(namenode_sock, {
                'type': 'upload_file',
                'filename': os.path.basename(filepath),
                'num_chunks': len(chunks),
                'chunk_ids': [c['chunk_id'] for c in chunks]
            })
            
            response = receive_message(namenode_sock)
            namenode_sock.close()
            
            if response['status'] != 'success':
                return response
            
            # 2. Send chunks to datanodes
            for chunk_idx, chunk_info in enumerate(response['chunks']):
                chunk_id = chunk_info['chunk_id']
                chunk_data = chunks[chunk_idx]['data']
                
                # Send to all specified datanodes (Namenode returns ports)
                for datanode_port in chunk_info['datanodes']:
                    # FIX: Get the ZeroTier IP using the port map
                    datanode_ip = DATANODE_PORT_TO_IP.get(datanode_port) 
                    
                    if not datanode_ip:
                         print(f"Error: Could not find IP for port {datanode_port}. Skipping chunk.")
                         continue
                         
                    try:
                        datanode_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        # FIX: Connect to the Datanode's ZeroTier IP
                        datanode_sock.connect((datanode_ip, datanode_port)) 
                        
                        send_message(datanode_sock, {
                            'type': 'store_chunk',
                            'chunk_id': chunk_id,
                            'size': len(chunk_data)
                        })
                        
                        # Send chunk data
                        datanode_sock.sendall(chunk_data)
                        
                        # Get acknowledgment
                        ack = receive_message(datanode_sock)
                        datanode_sock.close()
                        
                        print(f"Stored {chunk_id} on datanode {datanode_ip}:{datanode_port}")
                        
                    except Exception as e:
                        print(f"Error storing chunk {chunk_id} on datanode {datanode_ip}:{datanode_port}: {e}")
            
            return {'status': 'success', 'message': f'Uploaded {len(chunks)} chunks'}
            
        except Exception as e:
            return {'status': 'error', 'message': str(e)}
    
    def download_file(self, filename):
        """Download a file from HDFS"""
        try:
            # 1. Get chunk map from namenode
            namenode_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            namenode_sock.connect((self.namenode_host, self.namenode_port))
            
            send_message(namenode_sock, {
                'type': 'download_file',
                'filename': filename
            })
            
            response = receive_message(namenode_sock)
            namenode_sock.close()
            
            if response['status'] != 'success':
                return {'status': 'error', 'message': response.get('message', 'Unknown error')}
            
            # 2. Fetch all chunks
            all_chunks = []
            for chunk_info in response['chunks']:
                chunk_id = chunk_info['chunk_id']
                datanodes = chunk_info['datanodes'] # This is a list of ports
                
                chunk_data = None
                for datanode_port in datanodes:
                    # FIX: Get the ZeroTier IP using the port map
                    datanode_ip = DATANODE_PORT_TO_IP.get(datanode_port) 
                    if not datanode_ip: continue
                    
                    try:
                        datanode_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        # FIX: Connect to the Datanode's ZeroTier IP
                        datanode_sock.connect((datanode_ip, datanode_port)) 
                        
                        send_message(datanode_sock, {
                            'type': 'get_chunk',
                            'chunk_id': chunk_id
                        })
                        
                        # Receive chunk size
                        size_bytes = datanode_sock.recv(4)
                        size = int.from_bytes(size_bytes, byteorder='big')
                        
                        if size == 0:
                            datanode_sock.close()
                            continue
                        
                        # Receive chunk data
                        chunk_data = receive_file(datanode_sock, size)
                        datanode_sock.close()
                        
                        if chunk_data:
                            break # Chunk retrieved successfully
                        
                    except Exception as e:
                        print(f"Error fetching chunk {chunk_id} from {datanode_ip}:{datanode_port}: {e}")
                        continue
            
                if chunk_data is None:
                    return {'status': 'error', 'message': f'Could not retrieve chunk {chunk_id}'}
                
                all_chunks.append(chunk_data)
            
            # 3. Combine chunks
            file_data = b''.join(all_chunks)
            
            # 4. Save downloaded file
            output_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            with open(output_path, 'wb') as f:
                f.write(file_data)
            
            return {'status': 'success', 'filepath': output_path}
            
        except Exception as e:
            return {'status': 'error', 'message': str(e)}
    
    def list_files(self):
        """List all files in HDFS"""
        # Connection logic is correct: uses self.namenode_host and self.namenode_port
        try:
            namenode_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            namenode_sock.connect((self.namenode_host, self.namenode_port))
            
            send_message(namenode_sock, {'type': 'list_files'})
            response = receive_message(namenode_sock)
            namenode_sock.close()
            
            return response
            
        except Exception as e:
            return {'status': 'error', 'message': str(e)}
    
    def get_system_status(self):
        """Get status of all nodes"""
        # Connection logic is correct: uses self.namenode_host and self.namenode_port
        try:
            # 1. Get list of files
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((self.namenode_host, self.namenode_port))
            send_message(sock, {'type': 'list_files'})
            response = receive_message(sock)
            sock.close()
            
            files = response.get('files', [])
            
            # 2. Get detailed info for each file
            file_info = []
            for filename in files:
                try:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.connect((self.namenode_host, self.namenode_port))
                    
                    send_message(sock, {'type': 'get_file_info', 'filename': filename})
                    
                    file_response = receive_message(sock)
                    sock.close()
                    
                    if file_response and file_response.get('status') == 'success':
                        file_info.append({
                            'filename': filename,
                            'chunks': file_response.get('chunks', [])
                        })
                except Exception as e:
                    print(f"Error getting info for {filename}: {e}")
                    continue
            
            return {'status': 'success', 'files': file_info}
            
        except Exception as e:
            print(f"System status error: {e}")
            return {'status': 'error', 'message': str(e)}


# Global client instance
hdfs_client = HDFSClient()


# Flask Routes

@app.route('/')
def index():
    """Main dashboard"""
    return render_template('dashboard.html')


@app.route('/api/files', methods=['GET'])
def list_files_route():
    """List all files in HDFS"""
    result = hdfs_client.list_files()
    return jsonify(result)


@app.route('/api/status', methods=['GET'])
def get_status():
    """Get system status"""
    result = hdfs_client.get_system_status()
    return jsonify(result)


@app.route('/api/upload', methods=['POST'])
def upload_file_route():
    """Upload a file"""
    if 'file' not in request.files:
        return jsonify({'status': 'error', 'message': 'No file provided'}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'status': 'error', 'message': 'No file selected'}), 400
    
    temp_path = os.path.join(app.config['UPLOAD_FOLDER'], file.filename)
    file.save(temp_path)
    
    result = hdfs_client.upload_file(temp_path)
    
    # Clean up temp file
    if os.path.exists(temp_path):
        os.remove(temp_path)
    
    return jsonify(result)


@app.route('/api/download/<filename>', methods=['GET'])
def download_file_route(filename):
    """Download a file"""
    result = hdfs_client.download_file(filename)
    
    if result['status'] == 'success':
        # Send the file saved to the uploads folder as an attachment
        return send_file(result['filepath'], as_attachment=True)
    else:
        return jsonify(result), 404


if __name__ == '__main__':
    # FIX: Bind to the Client's ZeroTier IP for external access
    print(f"Starting HDFS Client on {CLIENT_IP}:{CLIENT_PORT}")
    app.run(host=CLIENT_IP, port=CLIENT_PORT, debug=True, threaded=True)
