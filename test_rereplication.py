#!/usr/bin/env python3
"""
Test Re-Replication After DataNode Failure

This script tests the re-replication mechanism by:
1. Uploading a test file
2. Verifying initial replication
3. Simulating DataNode failure (stop heartbeat)
4. Monitoring re-replication process
5. Verifying recovery
"""

import sys
import os
import time
import socket
import json

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import NAMENODE_IP, NAMENODE_PORT, REPLICATION_FACTOR

# =========================================================
# MESSAGE FRAMING
# =========================================================

def send_message(sock, message):
    """Send a JSON message with length prefix."""
    data = json.dumps(message).encode('utf-8')
    length_prefix = len(data).to_bytes(4, byteorder='big')
    sock.sendall(length_prefix + data)

def receive_message(sock):
    """Receive a JSON message with length prefix."""
    try:
        length_prefix = sock.recv(4)
        if not length_prefix:
            return None
        length = int.from_bytes(length_prefix, byteorder='big')
        
        data = b''
        while len(data) < length:
            chunk = sock.recv(min(length - len(data), 4096))
            if not chunk:
                return None
            data += chunk
        
        return json.loads(data.decode('utf-8'))
    except Exception as exc:
        print(f"[ERROR] Receive error: {exc}")
        return None

# =========================================================
# HELPER FUNCTIONS
# =========================================================

def get_cluster_status():
    """Get current cluster status from NameNode."""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((NAMENODE_IP, NAMENODE_PORT))
        send_message(sock, {"type": "cluster_status"})
        response = receive_message(sock)
        sock.close()
        return response
    except Exception as exc:
        print(f"[ERROR] Cannot get cluster status: {exc}")
        return None

def get_file_info(filename):
    """Get file metadata from NameNode."""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((NAMENODE_IP, NAMENODE_PORT))
        send_message(sock, {"type": "get_file_info", "filename": filename})
        response = receive_message(sock)
        sock.close()
        return response
    except Exception as exc:
        print(f"[ERROR] Cannot get file info: {exc}")
        return None

def list_files():
    """List all files in HDFS."""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((NAMENODE_IP, NAMENODE_PORT))
        send_message(sock, {"type": "list_files"})
        response = receive_message(sock)
        sock.close()
        return response
    except Exception as exc:
        print(f"[ERROR] Cannot list files: {exc}")
        return None

def audit_consistency():
    """Run consistency audit."""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((NAMENODE_IP, NAMENODE_PORT))
        send_message(sock, {"type": "audit_consistency"})
        response = receive_message(sock)
        sock.close()
        return response
    except Exception as exc:
        print(f"[ERROR] Cannot audit consistency: {exc}")
        return None

def print_chunk_replication(filename):
    """Print current replication status for a file's chunks."""
    response = get_file_info(filename)
    if response and response.get("status") == "success":
        metadata = response.get("metadata", {})
        chunks = metadata.get("chunks", [])
        
        print(f"\n[INFO] File: {filename}")
        print(f"[INFO] Total chunks: {len(chunks)}")
        print(f"[INFO] Replication factor: {REPLICATION_FACTOR}")
        
        for i, chunk in enumerate(chunks):
            chunk_id = chunk.get("chunk_id", "unknown")
            datanodes = chunk.get("datanodes", [])
            print(f"[INFO]   Chunk {i+1} ({chunk_id}): {len(datanodes)} replicas on {datanodes}")
    else:
        print(f"[ERROR] Cannot get file info for {filename}")

# =========================================================
# TEST SCENARIOS
# =========================================================

def test_1_initial_state():
    """Test 1: Check initial cluster state."""
    print("\n" + "="*60)
    print("TEST 1: Initial Cluster State")
    print("="*60)
    
    status = get_cluster_status()
    if status and status.get("status") == "success":
        cluster = status.get("cluster", {})
        datanodes = status.get("datanodes", [])
        
        print(f"[INFO] Total DataNodes: {cluster.get('total_datanodes', 0)}")
        print(f"[INFO] Alive DataNodes: {cluster.get('alive_datanodes', 0)}")
        print(f"[INFO] Total files: {cluster.get('total_files', 0)}")
        print(f"[INFO] Total chunks: {cluster.get('total_chunks', 0)}")
        print(f"[INFO] Under-replicated chunks: {cluster.get('under_replicated_chunks', 0)}")
        
        print("\n[INFO] DataNode Status:")
        for dn in datanodes:
            print(f"[INFO]   {dn.get('datanode_id', 'Unknown')} at {dn.get('address', 'N/A')}: {dn.get('status', 'UNKNOWN')}")
        
        if cluster.get('alive_datanodes', 0) < REPLICATION_FACTOR:
            print("[WARN] Not enough alive DataNodes for replication!")
            return False
        return True
    else:
        print("[ERROR] Cannot get cluster status")
        return False

def test_2_check_existing_files():
    """Test 2: Check existing files and their replication."""
    print("\n" + "="*60)
    print("TEST 2: Check Existing Files Replication")
    print("="*60)
    
    files = list_files()
    if files and files.get("status") == "success":
        file_list = files.get("files", [])
        print(f"[INFO] Found {len(file_list)} files")
        
        if not file_list:
            print("[WARN] No files found. Please upload a test file first.")
            return False
        
        # Check first 3 files
        for filename in file_list[:3]:
            print_chunk_replication(filename)
        
        return True
    else:
        print("[ERROR] Cannot list files")
        return False

def test_3_consistency_audit():
    """Test 3: Run consistency audit."""
    print("\n" + "="*60)
    print("TEST 3: Consistency Audit")
    print("="*60)
    
    audit = audit_consistency()
    if audit and audit.get("status") == "success":
        issues = audit.get("issues", [])
        issue_count = audit.get("issue_count", 0)
        
        print(f"[INFO] Consistency audit completed")
        print(f"[INFO] Issues found: {issue_count}")
        
        if issues:
            print("[INFO] Issues:")
            for issue in issues:
                print(f"[INFO]   - {issue}")
        
        return issue_count == 0
    else:
        print("[ERROR] Cannot run consistency audit")
        return False

def test_4_monitor_re_replication(duration=30):
    """Test 4: Monitor re-replication over time."""
    print("\n" + "="*60)
    print(f"TEST 4: Monitor Re-Replication ({duration}s)")
    print("="*60)
    
    print("[INFO] Monitoring cluster status for re-replication activity...")
    print("[INFO] This test assumes a DataNode has failed or is under-replicated.")
    
    start_time = time.time()
    last_under_replicated = -1
    
    while time.time() - start_time < duration:
        status = get_cluster_status()
        if status and status.get("status") == "success":
            cluster = status.get("cluster", {})
            under_replicated = cluster.get("under_replicated_chunks", 0)
            
            if under_replicated != last_under_replicated:
                print(f"[INFO] Under-replicated chunks: {under_replicated}")
                last_under_replicated = under_replicated
        
        time.sleep(5)
    
    print(f"[INFO] Monitoring complete. Final under-replicated chunks: {last_under_replicated}")
    
    if last_under_replicated == 0:
        print("[SUCCESS] All chunks are now properly replicated!")
        return True
    else:
        print(f"[WARN] {last_under_replicated} chunks are still under-replicated")
        return False

def test_5_final_verification():
    """Test 5: Final verification of cluster health."""
    print("\n" + "="*60)
    print("TEST 5: Final Cluster Verification")
    print("="*60)
    
    status = get_cluster_status()
    if status and status.get("status") == "success":
        cluster = status.get("cluster", {})
        datanodes = status.get("datanodes", [])
        
        alive = cluster.get('alive_datanodes', 0)
        total = cluster.get('total_datanodes', 0)
        under_replicated = cluster.get('under_replicated_chunks', 0)
        
        print(f"[INFO] Alive DataNodes: {alive}/{total}")
        print(f"[INFO] Under-replicated chunks: {under_replicated}")
        
        if alive == total and under_replicated == 0:
            print("[SUCCESS] Cluster is healthy!")
            return True
        else:
            print("[WARN] Cluster has issues:")
            if alive < total:
                print(f"[WARN]   - {total - alive} DataNode(s) are dead")
            if under_replicated > 0:
                print(f"[WARN]   - {under_replicated} chunk(s) are under-replicated")
            return False
    else:
        print("[ERROR] Cannot get cluster status")
        return False

# =========================================================
# MAIN TEST RUNNER
# =========================================================

def main():
    print("\n" + "="*60)
    print("HDFS Re-Replication Test Suite")
    print("="*60)
    print("\nThis test suite verifies re-replication functionality.")
    print("Prerequisites:")
    print("  1. NameNode must be running")
    print("  2. At least 2 DataNodes must be running")
    print("  3. Test files should be uploaded to HDFS")
    print("\nTo simulate DataNode failure:")
    print("  - Stop a DataNode process during the test")
    print("  - The re-replication worker should detect and fix under-replicated chunks")
    
    input("\nPress Enter to start tests...")
    
    results = []
    
    # Run tests
    results.append(("Initial State", test_1_initial_state()))
    results.append(("Existing Files", test_2_check_existing_files()))
    results.append(("Consistency Audit", test_3_consistency_audit()))
    
    print("\n[INFO] If you want to test re-replication, now is the time to:")
    print("[INFO]  1. Stop a DataNode (Ctrl+C in DataNode terminal)")
    print("[INFO]  2. Wait 10-20 seconds for re-replication to occur")
    
    response = input("\nDo you want to monitor re-replication? (y/n): ")
    if response.lower() == 'y':
        results.append(("Re-Replication Monitor", test_4_monitor_re_replication(30)))
    
    results.append(("Final Verification", test_5_final_verification()))
    
    # Print summary
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "[PASS]" if result else "[FAIL]"
        print(f"{status} {test_name}")
    
    print(f"\nTotal: {passed}/{total} tests passed")
    
    if passed == total:
        print("[SUCCESS] All tests passed!")
        return 0
    else:
        print("[WARN] Some tests failed. Check the output above.")
        return 1

if __name__ == "__main__":
    sys.exit(main())
