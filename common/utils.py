# HDFS/common/utils.py
"""
Reliable network utilities used by the NameNode, DataNodes and Client.

Phase 0:
- Provides reliable TCP reads.
- Prevents partial TCP packets from corrupting messages.
- Provides reliable JSON message framing.
- Provides reliable binary file/chunk transfers.
"""

import json
import struct
import socket
import hashlib


# ============================================================
# TCP HELPERS
# ============================================================

def recv_exact(sock: socket.socket, size: int) -> bytes:
    """
    Receive exactly `size` bytes from a TCP socket.

    TCP does not guarantee that one recv() call returns all
    requested bytes, so we keep reading until the required
    number of bytes has been received.
    """
    if size < 0:
        raise ValueError("size cannot be negative")

    data = bytearray()

    while len(data) < size:
        chunk = sock.recv(size - len(data))

        if not chunk:
            raise ConnectionError(
                f"Connection closed while receiving data "
                f"(received {len(data)} of {size} bytes)"
            )

        data.extend(chunk)

    return bytes(data)


# ============================================================
# JSON MESSAGE PROTOCOL
# ============================================================

def send_message(sock: socket.socket, message: dict) -> None:
    """
    Send a JSON message with a 4-byte big-endian length prefix.

    Wire format:

        [4-byte message length][JSON payload]
    """
    try:
        data = json.dumps(message).encode("utf-8")

        # 4-byte unsigned integer, network/big-endian order
        header = struct.pack(">I", len(data))

        sock.sendall(header)
        sock.sendall(data)

    except (OSError, TypeError, ValueError) as exc:
        print(f"Error sending message: {exc}")
        raise


def receive_message(sock: socket.socket):
    """
    Receive one complete length-prefixed JSON message.

    Returns:
        dict/list/etc. decoded from JSON

    Returns None only when the peer closes the connection before
    sending any message bytes.
    """
    try:
        # First receive the complete 4-byte header.
        header = sock.recv(4)

        if not header:
            return None

        # A partial header means the connection was interrupted.
        while len(header) < 4:
            chunk = sock.recv(4 - len(header))

            if not chunk:
                raise ConnectionError(
                    "Connection closed while receiving message header"
                )

            header += chunk

        message_length = struct.unpack(">I", header)[0]

        # Basic protection against obviously invalid messages.
        if message_length == 0:
            raise ValueError("Received an empty JSON message")

        # Receive the complete JSON payload.
        payload = recv_exact(sock, message_length)

        return json.loads(payload.decode("utf-8"))

    except (OSError, ConnectionError, ValueError, json.JSONDecodeError) as exc:
        print(f"Error receiving message: {exc}")
        raise


# ============================================================
# BINARY FILE / CHUNK TRANSFER
# ============================================================

def receive_file(sock: socket.socket, size: int) -> bytes:
    """
    Receive exactly `size` bytes of binary data.

    Used for HDFS chunk/file transfers.
    """
    if size < 0:
        raise ValueError("File size cannot be negative")

    if size == 0:
        return b""

    try:
        return recv_exact(sock, size)

    except (OSError, ConnectionError) as exc:
        print(f"Error receiving file data: {exc}")
        raise


# ============================================================
# PHASE 4: CHECKSUM UTILITIES
# ============================================================

def calculate_checksum(data: bytes) -> str:
    """
    Calculate SHA-256 checksum for binary data.

    Returns:
        str: Hexadecimal SHA-256 checksum
    """
    return hashlib.sha256(data).hexdigest()


def verify_checksum(data: bytes, expected_checksum: str) -> bool:
    """
    Verify that data matches the expected checksum.

    Args:
        data: Binary data to verify
        expected_checksum: Expected SHA-256 checksum (hex string)

    Returns:
        bool: True if checksum matches, False otherwise
    """
    actual_checksum = calculate_checksum(data)
    return actual_checksum == expected_checksum
