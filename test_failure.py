#!/usr/bin/env python3
"""
Failure Test Suite - Kill Nodes, Verify Detection/Recovery

This script tests fault tolerance by:
1. Checking initial cluster health
2. Simulating DataNode failures
3. Verifying detection and health status updates
4. Testing operations during partial failure
5. Verifying recovery after node restart
"""

import sys
import os
import time
import socket
import json
import subprocess

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import NAMENODE_IP, NAMENODE_PORT, REPLICATION_FACTOR, HEARTBEAT_TIMEOUT

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

def wait_for_datanode_death(datanode_address, timeout=30):
    """Wait for a DataNode to be marked as DEAD."""
    print(f"[INFO] Waiting for DataNode {datanode_address} to be marked DEAD...")
    start_time = time.time()
    
    while time.time() - start_time < timeout:
        status = get_cluster_status()
        if status and status.get("status") == "success":
            datanodes = status.get("datanodes", [])
            for dn in datanodes:
                if dn.get("address") == datanode_address:
                    if dn.get("status") == "DEAD":
                        print(f"[INFO] DataNode {datanode_address} marked as DEAD")
                        return True
                    break
        time.sleep(2)
    
    print(f"[WARN] DataNode {datanode_address} not marked DEAD within timeout")
    return False

def wait_for_datanode_recovery(datanode_address, timeout=30):
    """Wait for a DataNode to be marked as ALIVE."""
    print(f"[INFO] Waiting for DataNode {datanode_address} to be marked ALIVE...")
    start_time = time.time()
    
    while time.time() - start_time < timeout:
        status = get_cluster_status()
        if status and status.get("status") == "success":
            datanodes = status.get("datanodes", [])
            for dn in datanodes:
                if dn.get("address") == datanode_address:
                    if dn.get("status") == "ALIVE":
                        print(f"[INFO] DataNode {datanode_address} marked as ALIVE")
                        return True
                    break
        time.sleep(2)
    
    print(f"[WARN] DataNode {datanode_address} not marked ALIVE within timeout")
    return False

# =========================================================
# TEST SCENARIOS
# =========================================================

def test_1_initial_health():
    """Test 1: Check initial cluster health."""
    print("\n" + "="*60)
    print("TEST 1: Initial Cluster Health")
    print("="*60)
    
    status = get_cluster_status()
    if status and status.get("status") == "success":
        cluster = status.get("cluster", {})
        datanodes = status.get("datanodes", [])
        
        alive = cluster.get('alive_datanodes', 0)
        total = cluster.get('total_datanodes', 0)
        
        print(f"[INFO] Total DataNodes: {total}")
        print(f"[INFO] Alive DataNodes: {alive}")
        
        if alive < REPLICATION_FACTOR:
            print(f"[WARN] Only {alive} DataNodes alive (need {REPLICATION_FACTOR} for replication)")
            return False
        
        print("[INFO] Cluster is healthy")
        return True
    else:
        print("[ERROR] Cannot get cluster status")
        return False

def test_2_dataNode_detection():
    """Test 2: Verify DataNode failure detection."""
    print("\n" + "="*60)
    print("TEST 2: DataNode Failure Detection")
    print("="*60)
    
    print("[INFO] This test requires you to manually stop a DataNode.")
    print("[INFO] Instructions:")
    print("[INFO]  1. Go to a DataNode terminal")
    print("[INFO]  2. Press Ctrl+C to stop it")
    print("[INFO]  3. Return here and press Enter")
    
    input("\nPress Enter after stopping a DataNode...")
    
    status = get_cluster_status()
    if status and status.get("status") == "success":
        cluster = status.get("cluster", {})
        datanodes = status.get("datanodes", [])
        
        alive_before = cluster.get('alive_datanodes', 0)
        print(f"[INFO] Alive DataNodes before: {alive_before}")
        
        # Wait for detection
        print(f"[INFO] Waiting for detection (timeout: {HEARTBEAT_TIMEOUT + 10}s)...")
        time.sleep(HEARTBEAT_TIMEOUT + 10)
        
        status_after = get_cluster_status()
        if status_after and status_after.get("status") == "success":
            cluster_after = status_after.get("cluster", {})
            alive_after = cluster_after.get('alive_datanodes', 0)
            
            print(f"[INFO] Alive DataNodes after: {alive_after}")
            
            if alive_after < alive_before:
                print("[SUCCESS] DataNode failure detected!")
                return True
            else:
                print("[WARN] DataNode failure not detected")
                return False
    else:
        print("[ERROR] Cannot get cluster status")
        return False

def test_3_operations_during_failure():
    """Test 3: Test operations during partial failure."""
    print("\n" + "="*60)
    print("TEST 3: Operations During Partial Failure")
    print("="*60)
    
    print("[INFO] Testing file operations with reduced cluster...")
    
    # Test list files
    files = list_files()
    if files and files.get("status") == "success":
        file_list = files.get("files", [])
        print(f"[INFO] List files: {len(file_list)} files found")
    else:
        print("[ERROR] Cannot list files")
        return False
    
    # Test file info on existing files
    if file_list:
        test_file = file_list[0]
        info = get_file_info(test_file)
        if info and info.get("status") == "success":
            print(f"[INFO] Get file info: Success for {test_file}")
        else:
            print(f"[WARN] Cannot get file info for {test_file}")
    
    print("[INFO] Operations completed during partial failure")
    return True

def test_4_dataNode_recovery():
    """Test 4: Verify DataNode recovery after restart."""
    print("\n" + "="*60)
    print("TEST 4: DataNode Recovery")
    print("="*60)
    
    print("[INFO] This test requires you to restart the stopped DataNode.")
    print("[INFO] Instructions:")
    print("[INFO]  1. Go to the DataNode terminal")
    print("[INFO]  2. Run: python datanodeX.py")
    print("[INFO]  3. Return here and press Enter")
    
    input("\nPress Enter after restarting the DataNode...")
    
    status = get_cluster_status()
    if status and status.get("status") == "success":
        cluster = status.get("cluster", {})
        datanodes = status.get("datanodes", [])
        
        alive_before = cluster.get('alive_datanodes', 0)
        print(f"[INFO] Alive DataNodes before: {alive_before}")
        
        # Wait for recovery
        print("[INFO] Waiting for DataNode registration...")
        time.sleep(5)
        
        status_after = get_cluster_status()
        if status_after and status_after.get("status") == "success":
            cluster_after = status_after.get("cluster", {})
            alive_after = cluster_after.get('alive_datanodes', 0)
            
            print(f"[INFO] Alive DataNodes after: {alive_after}")
            
            if alive_after > alive_before:
                print("[SUCCESS] DataNode recovered!")
                return True
            else:
                print("[WARN] DataNode not recovered")
                return False
    else:
        print("[ERROR] Cannot get cluster status")
        return False

def test_5_final_health():
    """Test 5: Final cluster health check."""
    print("\n" + "="*60)
    print("TEST 5: Final Cluster Health")
    print("="*60)
    
    status = get_cluster_status()
    if status and status.get("status") == "success":
        cluster = status.get("cluster", {})
        datanodes = status.get("datanodes", [])
        
        alive = cluster.get('alive_datanodes', 0)
        total = cluster.get('total_datanodes', 0)
        under_replicated = cluster.get('under_replicated_chunks', 0)
        
        print(f"[INFO] Total DataNodes: {total}")
        print(f"[INFO] Alive DataNodes: {alive}")
        print(f"[INFO] Under-replicated chunks: {under_replicated}")
        
        if alive == total:
            print("[SUCCESS] All DataNodes are alive")
        else:
            print(f"[WARN] {total - alive} DataNode(s) still dead")
        
        if under_replicated == 0:
            print("[SUCCESS] All chunks are properly replicated")
        else:
            print(f"[WARN] {under_replicated} chunk(s) still under-replicated")
        
        return alive == total and under_replicated == 0
    else:
        print("[ERROR] Cannot get cluster status")
        return False

# =========================================================
# MAIN TEST RUNNER
# =========================================================

def main():
    print("\n" + "="*60)
    print("HDFS Failure Test Suite")
    print("="*60)
    print("\nThis test suite verifies fault tolerance and recovery.")
    print("Prerequisites:")
    print("  1. NameNode must be running")
    print("  2. All DataNodes must be running initially")
    print("  3. Test files should exist in HDFS")
    print("\nThis test involves manual steps:")
    print("  - You will be asked to stop a DataNode")
    print("  - You will be asked to restart the DataNode")
    
    input("\nPress Enter to start tests...")
    
    results = []
    
    # Run tests
    results.append(("Initial Health", test_1_initial_health()))
    
    # Ask if user wants to proceed with failure tests
    response = input("\nDo you want to proceed with DataNode failure tests? (y/n): ")
    if response.lower() == 'y':
        results.append(("Failure Detection", test_2_dataNode_detection()))
        results.append(("Operations During Failure", test_3_operations_during_failure()))
        results.append(("DataNode Recovery", test_4_dataNode_recovery()))
    
    results.append(("Final Health", test_5_final_health()))
    
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
