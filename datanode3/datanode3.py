import os
import sys
import socket
import threading
import time
import shutil


PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..")
)

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


from config import (
    NAMENODE_ADDRESS,
    DATANODE3_ADDRESS,
    DATANODE3_BIND_IP,
    DATANODE3_PORT,
    HEARTBEAT_INTERVAL,
)

from common.utils import (
    send_message,
    receive_message,
    receive_file,
    calculate_checksum,
    verify_checksum,
)
from common.kafka_producer import publish_event


DATANODE_ID = "DN-5004"

STORAGE_DIR = os.path.join(
    os.path.dirname(__file__),
    "storage",
)

os.makedirs(
    STORAGE_DIR,
    exist_ok=True,
)


def get_chunk_inventory():
    """
    Return all chunks physically stored on this DataNode.
    """

    inventory = []

    try:

        for filename in os.listdir(
            STORAGE_DIR
        ):

            chunk_path = os.path.join(
                STORAGE_DIR,
                filename,
            )

            if not os.path.isfile(
                chunk_path
            ):
                continue

            inventory.append(
                {
                    "chunk_id": filename,
                    "size": os.path.getsize(
                        chunk_path
                    ),
                }
            )

    except Exception as exc:

        print(
            f"[DataNode 3] Inventory scan error: "
            f"{exc}"
        )

    return inventory


def get_storage_capacity():
    """
    Return storage capacity information for this DataNode.
    """
    try:
        total, used, free = shutil.disk_usage(STORAGE_DIR)
        return {
            "total_bytes": total,
            "used_bytes": used,
            "free_bytes": free,
            "chunk_count": len(get_chunk_inventory()),
        }
    except Exception as exc:
        print(
            f"[DataNode 3] Capacity check error: "
            f"{exc}"
        )
        return {
            "total_bytes": 0,
            "used_bytes": 0,
            "free_bytes": 0,
            "chunk_count": 0,
        }


def handle_client_request(conn, addr):
    """
    Handle a request from NameNode or another DataNode.
    """

    try:

        request = receive_message(conn)

        if not request:
            return

        request_type = request.get("type")
        chunk_id = request.get("chunk_id")

        chunk_path = os.path.join(
            STORAGE_DIR,
            chunk_id,
        )

        # =================================================
        # STORE CHUNK
        # =================================================

        if request_type == "store_chunk":

            chunk_size = request.get("size")

            print(
                f"[DataNode 3] Storing {chunk_id} "
                f"({chunk_size} bytes)"
            )

            chunk_data = receive_file(
                conn,
                chunk_size,
            )

            checksum = calculate_checksum(
                chunk_data
            )

            with open(
                chunk_path,
                "wb",
            ) as f:
                f.write(chunk_data)

            print(
                f"[DataNode 3] Stored {chunk_id} "
                f"(checksum: {checksum})"
            )

            send_message(
                conn,
                {
                    "status": "success",
                    "checksum": checksum,
                },
            )
            publish_event(
                "chunk.created",
                DATANODE_ID,
                {"chunk_id": chunk_id, "size": chunk_size, "checksum": checksum},
            )

        # =================================================
        # GET CHUNK
        # =================================================

        elif request_type == "get_chunk":

            if not os.path.exists(
                chunk_path
            ):

                send_message(
                    conn,
                    {
                        "status": "error",
                        "message": (
                            f"Chunk {chunk_id} "
                            "not found."
                        ),
                    },
                )
                return

            with open(
                chunk_path,
                "rb",
            ) as f:
                chunk_data = f.read()

            # Phase 15: SHA-256 checksum
            checksum = calculate_checksum(chunk_data)

            file_size = len(chunk_data)

            # Send file size, checksum, and data
            send_message(
                conn,
                {
                    "status": "success",
                    "file_size": file_size,
                    "checksum": checksum,
                }
            )

            conn.sendall(chunk_data)

            print(
                f"[DataNode 3] Sending {chunk_id} "
                f"({file_size} bytes, "
                f"checksum: {checksum})"
            )

        # =================================================
        # COPY CHUNK
        # =================================================

        elif request_type == "copy_chunk":

            destination_ip = request.get(
                "destination_ip"
            )

            destination_port = request.get(
                "destination_port"
            )

            if (
                not destination_ip
                or destination_port is None
            ):

                send_message(
                    conn,
                    {
                        "status": "error",
                        "message": (
                            "Destination address "
                            "is required."
                        ),
                    },
                )
                return

            if not os.path.exists(
                chunk_path
            ):

                send_message(
                    conn,
                    {
                        "status": "error",
                        "message": (
                            f"Chunk {chunk_id} "
                            "not found locally "
                            "for copy."
                        ),
                    },
                )
                return

            with open(
                chunk_path,
                "rb",
            ) as f:

                chunk_data = f.read()

            file_size = len(
                chunk_data
            )

            destination_addr = (
                destination_ip,
                int(destination_port),
            )

            print(
                f"[DataNode 3] Copying "
                f"{chunk_id} to "
                f"{destination_ip}:"
                f"{destination_port}"
            )

            with socket.socket(
                socket.AF_INET,
                socket.SOCK_STREAM,
            ) as dest_sock:

                dest_sock.connect(
                    destination_addr
                )

                send_message(
                    dest_sock,
                    {
                        "type": "store_chunk",
                        "chunk_id": chunk_id,
                        "size": file_size,
                    },
                )

                dest_sock.sendall(
                    chunk_data
                )

                destination_response = (
                    receive_message(
                        dest_sock
                    )
                )

                if (
                    destination_response
                    and destination_response.get(
                        "status"
                    ) == "success"
                ):

                    print(
                        f"[DataNode 3] Successfully "
                        f"copied {chunk_id}"
                    )

                    send_message(
                        conn,
                        {
                            "status": "success",
                            "message": (
                                "Chunk copied "
                                "successfully."
                            ),
                        },
                    )

                else:

                    send_message(
                        conn,
                        {
                            "status": "error",
                            "message": (
                                "Destination DataNode "
                                "failed to store chunk."
                            ),
                        },
                    )

        # =================================================
        # DELETE CHUNK
        # =================================================

        elif request_type == "delete_chunk":

            if not os.path.exists(
                chunk_path
            ):

                send_message(
                    conn,
                    {
                        "status": "error",
                        "message": (
                            f"Chunk {chunk_id} "
                            "not found."
                        ),
                    },
                )
                return

            try:
                os.remove(chunk_path)
                print(
                    f"[DataNode 3] Deleted chunk {chunk_id}"
                )
                send_message(
                    conn,
                    {
                        "status": "success",
                        "message": f"Deleted {chunk_id}",
                    },
                )
                publish_event(
                    "chunk.deleted",
                    DATANODE_ID,
                    {"chunk_id": chunk_id},
                )
            except Exception as exc:
                print(
                    f"[DataNode 3] Failed to delete {chunk_id}: {exc}"
                )
                send_message(
                    conn,
                    {
                        "status": "error",
                        "message": str(exc),
                    },
                )

        # =================================================
        # GET INVENTORY
        # =================================================

        elif request_type == "get_inventory":

            inventory = get_chunk_inventory()

            send_message(
                conn,
                {
                    "status": "success",
                    "datanode_id": DATANODE_ID,
                    "port": DATANODE3_ADDRESS[1],
                    "inventory": inventory,
                },
            )

        else:

            send_message(
                conn,
                {
                    "status": "error",
                    "message": (
                        f"Unknown request type: "
                        f"{request_type}"
                    ),
                },
            )

    except Exception as exc:

        print(
            f"[DataNode 3] Request handler error: "
            f"{exc}"
        )

    finally:

        conn.close()


def send_heartbeat():
    """
    Send periodic heartbeats to NameNode.
    """

    while True:

        time.sleep(HEARTBEAT_INTERVAL)

        try:

            sock = socket.socket(
                socket.AF_INET,
                socket.SOCK_STREAM,
            )

            sock.connect(NAMENODE_ADDRESS)

            inventory = get_chunk_inventory()
            capacity = get_storage_capacity()

            send_message(
                sock,
                {
                    "type": "heartbeat",
                    "datanode_id": DATANODE_ID,
                    "ip": DATANODE3_ADDRESS[0],
                    "port": DATANODE3_ADDRESS[1],
                    "inventory": inventory,
                    "capacity": capacity,
                },
            )

            sock.close()

            try:
                publish_event(
                    "heartbeat.received",
                    DATANODE_ID,
                    {
                        "host": DATANODE3_ADDRESS[0],
                        "port": DATANODE3_ADDRESS[1],
                        "status": "ALIVE",
                        "chunk_count": len(inventory),
                        "storage_used": capacity.get("used_bytes", 0),
                        "storage_free": capacity.get("free_bytes", 0),
                    },
                )
            except Exception as exc:
                print(f"[DataNode 3] Kafka heartbeat event error: {exc}")

        except Exception as exc:

            print(
                f"[DataNode 3] Heartbeat error: "
                f"{exc}"
            )


def register_with_namenode():
    """
    Register this DataNode with the NameNode.
    """

    try:

        sock = socket.socket(
            socket.AF_INET,
            socket.SOCK_STREAM,
        )

        sock.connect(NAMENODE_ADDRESS)

        capacity = get_storage_capacity()
        inventory = get_chunk_inventory()

        send_message(
            sock,
            {
                "type": "register_datanode",
                "datanode_id": DATANODE_ID,
                "ip": DATANODE3_ADDRESS[0],
                "port": DATANODE3_ADDRESS[1],
                "capacity": capacity,
                "inventory": inventory,
            },
        )

        response = receive_message(sock)

        sock.close()

        if response and response.get(
            "status"
        ) == "success":

            print(
                f"[DataNode 3] Successfully registered "
                f"with NameNode"
            )
            publish_event(
                "datanode.started",
                DATANODE_ID,
                {"host": DATANODE3_ADDRESS[0], "port": DATANODE3_ADDRESS[1], "capacity": capacity},
            )

        else:

            print(
                f"[DataNode 3] Registration failed: "
                f"{response}"
            )

    except Exception as exc:

        print(
            f"[DataNode 3] Registration error: "
            f"{exc}"
        )


def main():
    """
    Main DataNode 3 server loop.
    """

    print(
        f"[DataNode 3] Starting on "
        f"{DATANODE3_ADDRESS[0]}:"
        f"{DATANODE3_ADDRESS[1]}"
    )

    # Start server socket FIRST
    server_sock = socket.socket(
        socket.AF_INET,
        socket.SOCK_STREAM,
    )

    server_sock.setsockopt(
        socket.SOL_SOCKET,
        socket.SO_REUSEADDR,
        1,
    )

    try:
        server_sock.bind((DATANODE3_BIND_IP, DATANODE3_PORT))
    except OSError as exc:
        print(
            f"[DataNode 3] FATAL: Cannot bind to port {DATANODE3_PORT}: {exc}"
        )
        print(
            f"[DataNode 3] Another process may be using this port. "
            f"Please check and kill existing DataNode 3 processes."
        )
        return

    server_sock.listen(5)

    print(
        f"[DataNode 3] Listening for connections on port {DATANODE3_PORT}..."
    )

    # Only register AFTER successful binding
    register_with_namenode()

    # Start heartbeat thread
    heartbeat_thread = threading.Thread(
        target=send_heartbeat,
        daemon=True,
    )
    heartbeat_thread.start()

    try:

        while True:

            conn, addr = server_sock.accept()

            print(
                f"[DataNode 3] Connection from "
                f"{addr[0]}:{addr[1]}"
            )

            client_thread = threading.Thread(
                target=handle_client_request,
                args=(conn, addr),
                daemon=True,
            )

            client_thread.start()

    except KeyboardInterrupt:

        print(
            "\n[DataNode 3] Shutting down..."
        )

    finally:

        server_sock.close()


if __name__ == "__main__":
    main()
