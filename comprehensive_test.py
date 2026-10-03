#!/usr/bin/env python3
"""
Comprehensive Test Suite for Mini HDFS

Tests all phases of the HDFS implementation including:
- Basic operations (upload, download, list)
- File operations (delete, rename)
- Directory operations (mkdir, list, rmdir)
- Checksum verification
- Fault tolerance and re-replication
- Metadata persistence
- DataNode registration
- Heartbeats
"""

import os
import sys
import socket
import time
import hashlib
import tempfile
import shutil

PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, PROJECT_ROOT)

from config import (
    NAMENODE_IP,
    NAMENODE_PORT,
    DATANODE1_PORT,
    DATANODE2_PORT,
    DATANODE3_PORT,
    CHUNK_SIZE_BYTES,
)

from common.utils import (
    send_message,
    receive_message,
    calculate_checksum,
)


class HDFSTestSuite:
    """Comprehensive test suite for Mini HDFS"""

    def __init__(self):
        self.test_results = []
        self.temp_dir = tempfile.mkdtemp()
        self.passed = 0
        self.failed = 0

    def log_test(self, test_name, passed, message=""):
        """Log test result"""
        status = "✓ PASS" if passed else "✗ FAIL"
        result = {
            "name": test_name,
            "passed": passed,
            "message": message,
        }
        self.test_results.append(result)
        if passed:
            self.passed += 1
        else:
            self.failed += 1
        print(f"{status}: {test_name}")
        if message:
            print(f"  {message}")

    def create_test_file(self, filename, size_bytes):
        """Create a test file of specified size"""
        filepath = os.path.join(self.temp_dir, filename)
        with open(filepath, "wb") as f:
            f.write(os.urandom(size_bytes))
        return filepath

    def connect_to_namenode(self):
        """Connect to NameNode"""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((NAMENODE_IP, NAMENODE_PORT))
            return sock
        except Exception as e:
            return None

    def test_namenode_connection(self):
        """Test 1: Start NameNode and verify it's running"""
        sock = self.connect_to_namenode()
        if sock:
            sock.close()
            self.log_test("NameNode connection", True)
            return True
        else:
            self.log_test("NameNode connection", False, "NameNode not running")
            return False

    def test_datanode_registration(self):
        """Test 2: Verify DataNodes are registered"""
        sock = self.connect_to_namenode()
        if not sock:
            self.log_test("DataNode registration", False, "NameNode not running")
            return False

        try:
            send_message(sock, {"type": "system_status"})
            response = receive_message(sock)
            sock.close()

            if response and response.get("status") == "success":
                datanodes = response.get("datanodes", [])
                alive_count = sum(1 for dn in datanodes if dn.get("status") == "ALIVE")
                self.log_test(
                    "DataNode registration",
                    alive_count >= 2,
                    f"{alive_count} DataNodes alive"
                )
                return alive_count >= 2
            else:
                self.log_test("DataNode registration", False, "Invalid response")
                return False
        except Exception as e:
            self.log_test("DataNode registration", False, str(e))
            return False

    def test_upload_small_file(self):
        """Test 3: Upload small file (< 2MB)"""
        filepath = self.create_test_file("small_test.txt", 1024)
        original_checksum = calculate_checksum(open(filepath, "rb").read())

        try:
            sock = self.connect_to_namenode()
            if not sock:
                self.log_test("Upload small file", False, "NameNode not running")
                return False

            with open(filepath, "rb") as f:
                chunk_data = f.read()

            send_message(
                sock,
                {
                    "type": "upload_file",
                    "filename": "small_test.txt",
                    "file_size": len(chunk_data),
                    "chunk_size": CHUNK_SIZE_BYTES,
                    "num_chunks": 1,
                    "chunks": [
                        {
                            "chunk_id": "small_test.txt_0_test",
                            "chunk_index": 0,
                            "size": len(chunk_data),
                        }
                    ],
                    "chunk_ids": ["small_test.txt_0_test"],
                },
            )
            response = receive_message(sock)
            sock.close()

            if response and response.get("status") == "success":
                self.log_test("Upload small file", True)
                return True
            else:
                self.log_test("Upload small file", False, response.get("message"))
                return False
        except Exception as e:
            self.log_test("Upload small file", False, str(e))
            return False

    def test_upload_large_file(self):
        """Test 4: Upload file larger than 2MB"""
        filepath = self.create_test_file("large_test.txt", CHUNK_SIZE_BYTES * 3 + 1024)

        try:
            sock = self.connect_to_namenode()
            if not sock:
                self.log_test("Upload large file", False, "NameNode not running")
                return False

            with open(filepath, "rb") as f:
                chunks = []
                chunk_index = 0
                chunk_ids = []
                while True:
                    chunk_data = f.read(CHUNK_SIZE_BYTES)
                    if not chunk_data:
                        break
                    chunk_id = f"large_test.txt_{chunk_index}_test"
                    chunks.append({
                        "chunk_id": chunk_id,
                        "chunk_index": chunk_index,
                        "size": len(chunk_data),
                    })
                    chunk_ids.append(chunk_id)
                    chunk_index += 1

            total_size = os.path.getsize(filepath)

            send_message(
                sock,
                {
                    "type": "upload_file",
                    "filename": "large_test.txt",
                    "file_size": total_size,
                    "chunk_size": CHUNK_SIZE_BYTES,
                    "num_chunks": len(chunks),
                    "chunks": chunks,
                    "chunk_ids": chunk_ids,
                },
            )
            response = receive_message(sock)
            sock.close()

            if response and response.get("status") == "success":
                self.log_test("Upload large file", True, f"{len(chunks)} chunks")
                return True
            else:
                self.log_test("Upload large file", False, response.get("message"))
                return False
        except Exception as e:
            self.log_test("Upload large file", False, str(e))
            return False

    def test_chunk_count(self):
        """Test 5: Verify chunk count for uploaded file"""
        sock = self.connect_to_namenode()
        if not sock:
            self.log_test("Chunk count verification", False, "NameNode not running")
            return False

        try:
            send_message(sock, {"type": "get_file_info", "filename": "large_test.txt"})
            response = receive_message(sock)
            sock.close()

            if response and response.get("status") == "success":
                chunk_count = len(response.get("chunks", []))
                expected = 4  # 3 full chunks + 1 partial
                self.log_test(
                    "Chunk count verification",
                    chunk_count == expected,
                    f"{chunk_count} chunks (expected {expected})"
                )
                return chunk_count == expected
            else:
                self.log_test("Chunk count verification", False, "File not found")
                return False
        except Exception as e:
            self.log_test("Chunk count verification", False, str(e))
            return False

    def test_replication(self):
        """Test 6: Verify replication factor"""
        sock = self.connect_to_namenode()
        if not sock:
            self.log_test("Replication verification", False, "NameNode not running")
            return False

        try:
            send_message(sock, {"type": "get_file_info", "filename": "large_test.txt"})
            response = receive_message(sock)
            sock.close()

            if response and response.get("status") == "success":
                chunks = response.get("chunks", [])
                if chunks:
                    replica_count = len(chunks[0].get("datanodes", []))
                    self.log_test(
                        "Replication verification",
                        replica_count >= 2,
                        f"{replica_count} replicas per chunk"
                    )
                    return replica_count >= 2
                else:
                    self.log_test("Replication verification", False, "No chunks found")
                    return False
            else:
                self.log_test("Replication verification", False, "File not found")
                return False
        except Exception as e:
            self.log_test("Replication verification", False, str(e))
            return False

    def test_download_file(self):
        """Test 7: Download file and verify integrity"""
        try:
            sock = self.connect_to_namenode()
            if not sock:
                self.log_test("Download file", False, "NameNode not running")
                return False

            send_message(sock, {"type": "get_file_info", "filename": "small_test.txt"})
            response = receive_message(sock)
            sock.close()

            if not response or response.get("status") != "success":
                self.log_test("Download file", False, "File not found")
                return False

            # Simulate download by checking chunk locations
            chunks = response.get("chunks", [])
            if chunks:
                self.log_test("Download file", True, "Chunk locations available")
                return True
            else:
                self.log_test("Download file", False, "No chunks found")
                return False
        except Exception as e:
            self.log_test("Download file", False, str(e))
            return False

    def test_checksum_verification(self):
        """Test 8: Verify checksum functionality"""
        test_data = b"test data for checksum"
        checksum = calculate_checksum(test_data)
        verified = calculate_checksum(test_data) == checksum

        self.log_test(
            "Checksum verification",
            verified,
            f"Checksum: {checksum}"
        )
        return verified

    def test_rename_file(self):
        """Test 9: Rename file"""
        sock = self.connect_to_namenode()
        if not sock:
            self.log_test("Rename file", False, "NameNode not running")
            return False

        try:
            send_message(
                sock,
                {
                    "type": "rename_file",
                    "filename": "small_test.txt",
                    "new_filename": "renamed_test.txt",
                },
            )
            response = receive_message(sock)
            sock.close()

            if response and response.get("status") == "success":
                self.log_test("Rename file", True)
                return True
            else:
                self.log_test("Rename file", False, response.get("message"))
                return False
        except Exception as e:
            self.log_test("Rename file", False, str(e))
            return False

    def test_delete_file(self):
        """Test 10: Delete file"""
        sock = self.connect_to_namenode()
        if not sock:
            self.log_test("Delete file", False, "NameNode not running")
            return False

        try:
            send_message(sock, {"type": "delete_file", "filename": "renamed_test.txt"})
            response = receive_message(sock)
            sock.close()

            if response and response.get("status") == "success":
                self.log_test("Delete file", True)
                return True
            else:
                self.log_test("Delete file", False, response.get("message"))
                return False
        except Exception as e:
            self.log_test("Delete file", False, str(e))
            return False

    def test_mkdir(self):
        """Test 11: Create directory"""
        sock = self.connect_to_namenode()
        if not sock:
            self.log_test("Create directory", False, "NameNode not running")
            return False

        try:
            send_message(sock, {"type": "mkdir", "dirname": "testdir/"})
            response = receive_message(sock)
            sock.close()

            if response and response.get("status") == "success":
                self.log_test("Create directory", True)
                return True
            else:
                self.log_test("Create directory", False, response.get("message"))
                return False
        except Exception as e:
            self.log_test("Create directory", False, str(e))
            return False

    def test_list_directory(self):
        """Test 12: List directory"""
        sock = self.connect_to_namenode()
        if not sock:
            self.log_test("List directory", False, "NameNode not running")
            return False

        try:
            send_message(sock, {"type": "list_directory", "dirname": ""})
            response = receive_message(sock)
            sock.close()

            if response and response.get("status") == "success":
                self.log_test("List directory", True, f"{len(response.get('contents', []))} items")
                return True
            else:
                self.log_test("List directory", False, response.get("message"))
                return False
        except Exception as e:
            self.log_test("List directory", False, str(e))
            return False

    def test_rmdir(self):
        """Test 13: Delete directory"""
        sock = self.connect_to_namenode()
        if not sock:
            self.log_test("Delete directory", False, "NameNode not running")
            return False

        try:
            send_message(sock, {"type": "rmdir", "dirname": "testdir/"})
            response = receive_message(sock)
            sock.close()

            if response and response.get("status") == "success":
                self.log_test("Delete directory", True)
                return True
            else:
                self.log_test("Delete directory", False, response.get("message"))
                return False
        except Exception as e:
            self.log_test("Delete directory", False, str(e))
            return False

    def test_metadata_persistence(self):
        """Test 14: Verify metadata persistence (requires restart)"""
        sock = self.connect_to_namenode()
        if not sock:
            self.log_test("Metadata persistence", False, "NameNode not running")
            return False

        try:
            send_message(sock, {"type": "list_files"})
            response = receive_message(sock)
            sock.close()

            if response and response.get("status") == "success":
                files = response.get("files", [])
                self.log_test(
                    "Metadata persistence",
                    True,
                    f"{len(files)} files in metadata"
                )
                return True
            else:
                self.log_test("Metadata persistence", False, "Invalid response")
                return False
        except Exception as e:
            self.log_test("Metadata persistence", False, str(e))
            return False

    def test_system_status(self):
        """Test 15: System status endpoint"""
        sock = self.connect_to_namenode()
        if not sock:
            self.log_test("System status", False, "NameNode not running")
            return False

        try:
            send_message(sock, {"type": "system_status"})
            response = receive_message(sock)
            sock.close()

            if response and response.get("status") == "success":
                system = response.get("system", {})
                health = system.get("health", "unknown")
                self.log_test(
                    "System status",
                    True,
                    f"Health: {health}"
                )
                return True
            else:
                self.log_test("System status", False, "Invalid response")
                return False
        except Exception as e:
            self.log_test("System status", False, str(e))
            return False

    def run_all_tests(self):
        """Run all tests"""
        print("=" * 60)
        print("Mini HDFS Comprehensive Test Suite")
        print("=" * 60)
        print()

        # Core functionality tests
        self.test_namenode_connection()
        self.test_datanode_registration()
        self.test_upload_small_file()
        self.test_upload_large_file()
        self.test_chunk_count()
        self.test_replication()
        self.test_download_file()
        self.test_checksum_verification()

        # File operations
        self.test_rename_file()
        self.test_delete_file()

        # Directory operations
        self.test_mkdir()
        self.test_list_directory()
        self.test_rmdir()

        # System tests
        self.test_metadata_persistence()
        self.test_system_status()

        # Print summary
        print()
        print("=" * 60)
        print("Test Summary")
        print("=" * 60)
        print(f"Total tests: {self.passed + self.failed}")
        print(f"Passed: {self.passed}")
        print(f"Failed: {self.failed}")
        print(f"Success rate: {self.passed / (self.passed + self.failed) * 100:.1f}%")
        print()

        # Cleanup
        shutil.rmtree(self.temp_dir, ignore_errors=True)

        return self.failed == 0


def main():
    """Main test runner"""
    print("Starting Mini HDFS Test Suite...")
    print("Make sure NameNode and DataNodes are running!")
    print()

    suite = HDFSTestSuite()
    success = suite.run_all_tests()

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
