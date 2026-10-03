import os
import sys
import socket
import threading
import time
import shutil


# =========================================================
# PROJECT ROOT / IMPORT SETUP
# =========================================================

PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..")
)

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


# =========================================================
# CONFIGURATION
# =========================================================

from config import (
    NAMENODE_ADDRESS,
    DATANODE1_ADDRESS,
    DATANODE1_BIND_IP,
    DATANODE1_PORT,
    HEARTBEAT_INTERVAL,
    CHUNK_SIZE_BYTES,
)

from common.utils import (
    send_message,
    receive_message,
    receive_file,
    calculate_checksum,
    verify_checksum,
)
from common.kafka_producer import publish_event


# =========================================================
# DATANODE STATE
# =========================================================

DATANODE_ID = "DN-5002"

STORAGE_DIR = os.path.join(
    os.path.dirname(__file__),
    "storage",
)

os.makedirs(
    STORAGE_DIR,
    exist_ok=True,
)


# =========================================================
# CHUNK INVENTORY
# =========================================================

def get_chunk_inventory():
    """
    Scan the local storage directory and return all chunks
    physically stored on this DataNode.
    """

    inventory = []

    try:

        for filename in os.listdir(STORAGE_DIR):

            chunk_path = os.path.join(
                STORAGE_DIR,
                filename,
            )

            if not os.path.isfile(chunk_path):
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
            f"[DataNode 1] Inventory scan error: "
            f"{exc}"
        )

    return inventory


# =========================================================
# PHASE 5: STORAGE CAPACITY
# =========================================================

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
        print(f"[DataNode 1] Capacity check error: {exc}")
        return {
            "total_bytes": 0,
            "used_bytes": 0,
            "free_bytes": 0,
            "chunk_count": 0,
        }


# =========================================================
# PHASE 5: DATA NODE REGISTRATION
# =========================================================

def register_with_namenode():
    """
    Register this DataNode with the NameNode on startup.
    """
    try:
        capacity = get_storage_capacity()
        inventory = get_chunk_inventory()
        
        with socket.socket(
            socket.AF_INET,
            socket.SOCK_STREAM,
        ) as s:
            s.connect(NAMENODE_ADDRESS)
            send_message(
                s,
                {
                    "type": "register_datanode",
                    "datanode_id": DATANODE_ID,
                    "ip": DATANODE1_ADDRESS[0],
                    "port": DATANODE1_ADDRESS[1],
                    "capacity": capacity,
                    "inventory": inventory,
                },
            )
            response = receive_message(s)
            
            if response and response.get("status") == "success":
                print(f"[DataNode 1] Successfully registered with NameNode")
                publish_event(
                    "datanode.started",
                    DATANODE_ID,
                    {"host": DATANODE1_ADDRESS[0], "port": DATANODE1_ADDRESS[1], "capacity": capacity},
                )
            else:
                print(f"[DataNode 1] Registration failed: {response}")
                
    except Exception as exc:
        print(f"[DataNode 1] Registration error: {exc}")


# =========================================================
# HEARTBEAT
# =========================================================

def send_heartbeat():
    """
    Periodically send health status and the complete
    local chunk inventory to the NameNode.
    """

    while True:

        try:

            inventory = get_chunk_inventory()
            capacity = get_storage_capacity()

            with socket.socket(
                socket.AF_INET,
                socket.SOCK_STREAM,
            ) as s:

                s.connect(
                    NAMENODE_ADDRESS
                )

                send_message(
                    s,
                    {
                        "type": "heartbeat",
                        "datanode_id": DATANODE_ID,
                        "ip": DATANODE1_ADDRESS[0],
                        "port": DATANODE1_ADDRESS[1],
                        "inventory": inventory,
                        "capacity": capacity,
                    },
                )
                publish_event(
                    "heartbeat.received",
                    DATANODE_ID,
                    {
                        "host": DATANODE1_ADDRESS[0],
                        "port": DATANODE1_ADDRESS[1],
                        "status": "ALIVE",
                        "chunk_count": len(inventory),
                        "storage_used": capacity.get("used_bytes", 0),
                        "storage_free": capacity.get("free_bytes", 0),
                    },
                )

        except Exception:
            # NameNode may be temporarily unavailable.
            # Continue heartbeat attempts.
            pass

        time.sleep(
            HEARTBEAT_INTERVAL
        )


# =========================================================
# CHUNK OPERATIONS
# =========================================================

def handle_chunk_operation(conn):
    """
    Handle chunk storage, retrieval and copy requests.
    """

    try:

        request = receive_message(
            conn
        )

        if not request:
            return

        request_type = request.get(
            "type"
        )

        chunk_id = request.get(
            "chunk_id"
        )

        if not chunk_id:
            send_message(
                conn,
                {
                    "status": "error",
                    "message": "chunk_id is required",
                },
            )
            return

        chunk_path = os.path.join(
            STORAGE_DIR,
            chunk_id,
        )

        # =================================================
        # STORE CHUNK
        # =================================================

        if request_type == "store_chunk":

            size = request.get(
                "size"
            )

            if size is None:
                send_message(
                    conn,
                    {
                        "status": "error",
                        "message": (
                            "Chunk size is required."
                        ),
                    },
                )
                return

            try:
                size = int(size)
            except (TypeError, ValueError):

                send_message(
                    conn,
                    {
                        "status": "error",
                        "message": (
                            "Invalid chunk size."
                        ),
                    },
                )
                return

            chunk_data = receive_file(
                conn,
                size,
            )

            if chunk_data is None:
                send_message(
                    conn,
                    {
                        "status": "error",
                        "message": (
                            "Failed to receive chunk data."
                        ),
                    },
                )
                return

            # Phase 4: Calculate checksum
            checksum = calculate_checksum(chunk_data)

            with open(
                chunk_path,
                "wb",
            ) as f:

                f.write(
                    chunk_data
                )

            actual_size = os.path.getsize(
                chunk_path
            )

            print(
                f"[DataNode 1] Stored chunk "
                f"{chunk_id} with size "
                f"{actual_size} bytes, "
                f"checksum: {checksum}"
            )

            send_message(
                conn,
                {
                    "status": "success",
                    "message": (
                        f"Stored {chunk_id}"
                    ),
                    "checksum": checksum,
                },
            )
            publish_event(
                "chunk.created",
                DATANODE_ID,
                {"chunk_id": chunk_id, "size": actual_size, "checksum": checksum},
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
                f"[DataNode 1] Sent chunk "
                f"{chunk_id} (size: {file_size}, checksum: {checksum})"
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
                f"[DataNode 1] Copying "
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

                    send_message(
                        conn,
                        {
                            "status": "success",
                            "message": (
                                f"Copied {chunk_id} "
                                "successfully"
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
                    f"[DataNode 1] Deleted chunk {chunk_id}"
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
                    f"[DataNode 1] Failed to delete {chunk_id}: {exc}"
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
                    "port": DATANODE1_ADDRESS[1],
                    "inventory": inventory,
                },
            )

        # =================================================
        # UNKNOWN REQUEST
        # =================================================

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
            f"[DataNode 1] Connection error: "
            f"{exc}"
        )

        try:

            send_message(
                conn,
                {
                    "status": "error",
                    "message": (
                        "DataNode internal error."
                    ),
                },
            )

        except Exception:
            pass

    finally:

        try:
            conn.close()
        except Exception:
            pass


# =========================================================
# DATANODE SERVER
# =========================================================

def start_datanode():

    server_socket = socket.socket(
        socket.AF_INET,
        socket.SOCK_STREAM,
    )

    server_socket.setsockopt(
        socket.SOL_SOCKET,
        socket.SO_REUSEADDR,
        1,
    )

    try:
        server_socket.bind((DATANODE1_BIND_IP, DATANODE1_PORT))
    except OSError as exc:
        print(
            f"[DataNode 1] FATAL: Cannot bind to port {DATANODE1_PORT}: {exc}"
        )
        print(
            f"[DataNode 1] Another process may be using this port. "
            f"Please check and kill existing DataNode 1 processes."
        )
        return

    server_socket.listen(
        10
    )

    print("=" * 60)
    print(
        f"DataNode 1 ({DATANODE_ID})"
    )
    print("=" * 60)

    print(
        f"Listening on "
        f"{DATANODE1_ADDRESS[0]}:"
        f"{DATANODE1_ADDRESS[1]}"
    )

    print(
        f"Storage: {STORAGE_DIR}"
    )

    print(
        f"Existing chunks: "
        f"{len(get_chunk_inventory())}"
    )

    print("=" * 60)

    # Phase 5: Register with NameNode on startup
    register_with_namenode()

    threading.Thread(
        target=send_heartbeat,
        daemon=True,
        name="DataNode1Heartbeat",
    ).start()

    while True:

        try:

            conn, address = (
                server_socket.accept()
            )

            threading.Thread(
                target=handle_chunk_operation,
                args=(conn,),
                daemon=True,
            ).start()

        except KeyboardInterrupt:

            print(
                "\n[DataNode 1] "
                "Shutdown requested."
            )

            break

        except Exception as exc:

            print(
                f"[DataNode 1] "
                f"Server error: {exc}"
            )

    server_socket.close()


# =========================================================
# MAIN
# =========================================================

if __name__ == "__main__":
    start_datanode()