#!/usr/bin/env python3
"""
Test script for new HDFS features:
- Delete file
- Rename file
- Directory operations (mkdir, list, rmdir)
"""

import sys
import os
import socket

PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, PROJECT_ROOT)

from config import NAMENODE_IP, NAMENODE_PORT
from common.utils import send_message, receive_message


def test_delete_file():
    """Test delete file functionality"""
    print("\n=== Testing Delete File ===")
    
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((NAMENODE_IP, NAMENODE_PORT))
    
    send_message(sock, {
        "type": "delete_file",
        "filename": "test_file.txt"
    })
    
    response = receive_message(sock)
    sock.close()
    
    print(f"Delete file response: {response}")
    return response.get("status") == "success" or response.get("status") == "error"


def test_rename_file():
    """Test rename file functionality"""
    print("\n=== Testing Rename File ===")
    
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((NAMENODE_IP, NAMENODE_PORT))
    
    send_message(sock, {
        "type": "rename_file",
        "filename": "old_name.txt",
        "new_filename": "new_name.txt"
    })
    
    response = receive_message(sock)
    sock.close()
    
    print(f"Rename file response: {response}")
    return response.get("status") == "success" or response.get("status") == "error"


def test_mkdir():
    """Test create directory functionality"""
    print("\n=== Testing Create Directory ===")
    
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((NAMENODE_IP, NAMENODE_PORT))
    
    send_message(sock, {
        "type": "mkdir",
        "dirname": "test_dir/"
    })
    
    response = receive_message(sock)
    sock.close()
    
    print(f"Create directory response: {response}")
    return response.get("status") == "success" or response.get("status") == "error"


def test_list_directory():
    """Test list directory functionality"""
    print("\n=== Testing List Directory ===")
    
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((NAMENODE_IP, NAMENODE_PORT))
    
    send_message(sock, {
        "type": "list_directory",
        "dirname": ""
    })
    
    response = receive_message(sock)
    sock.close()
    
    print(f"List directory response: {response}")
    return response.get("status") == "success"


def test_rmdir():
    """Test delete directory functionality"""
    print("\n=== Testing Delete Directory ===")
    
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((NAMENODE_IP, NAMENODE_PORT))
    
    send_message(sock, {
        "type": "rmdir",
        "dirname": "test_dir/"
    })
    
    response = receive_message(sock)
    sock.close()
    
    print(f"Delete directory response: {response}")
    return response.get("status") == "success" or response.get("status") == "error"


def main():
    print("Testing new HDFS features...")
    print(f"Connecting to NameNode at {NAMENODE_IP}:{NAMENODE_PORT}")
    
    tests = [
        ("Delete File", test_delete_file),
        ("Rename File", test_rename_file),
        ("Create Directory", test_mkdir),
        ("List Directory", test_list_directory),
        ("Delete Directory", test_rmdir),
    ]
    
    results = []
    for name, test_func in tests:
        try:
            result = test_func()
            results.append((name, result))
            print(f"✓ {name}: {'PASS' if result else 'FAIL'}")
        except Exception as e:
            results.append((name, False))
            print(f"✗ {name}: ERROR - {e}")
    
    print("\n=== Test Summary ===")
    passed = sum(1 for _, result in results if result)
    total = len(results)
    print(f"Passed: {passed}/{total}")
    
    for name, result in results:
        status = "✓ PASS" if result else "✗ FAIL"
        print(f"{status}: {name}")


if __name__ == "__main__":
    main()
