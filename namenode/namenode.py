# MINI_HDFS/namenode/namenode.py

import os
import sys
import socket
import threading
import time
import random
from datetime import datetime, timezone


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
    NAMENODE_BIND_IP,
    NAMENODE_PORT,
    DATANODE_ADDRESSES,
    DATANODE3_ADDRESS,
    HEARTBEAT_INTERVAL,
    HEARTBEAT_TIMEOUT,
    DATANODE_PORT_TO_IP,
    REPLICATION_FACTOR,
    CHUNK_SIZE_BYTES,
    REPLICATION_CHECK_INTERVAL,
    MAX_CONCURRENT_REPLICATIONS,
)

from common.utils import (
    send_message,
    receive_message,
    calculate_checksum,
)
from common.kafka_producer import publish_event

from namenode.metadata_manager import (
    load_metadata,
    save_metadata,
)


# =========================================================
# NAMENODE STATE
# =========================================================

file_metadata = load_metadata()
pending_uploads = {}


datanode_health = {
    address: {
        "status": "DEAD",
        "last_check": 0,
    }
    for address in DATANODE_ADDRESSES
}


# =========================================================
# PHASE 5: DYNAMIC DATANODE REGISTRY
# =========================================================

dynamic_datanodes = {}


# =========================================================
# DATANODE DISPLAY MAP
# =========================================================

DATANODE_MAP = {}

for index, address in enumerate(
    DATANODE_ADDRESSES,
    start=1,
):
    DATANODE_MAP[address] = f"DN-{5001 + index}"

# =========================================================
# DATANODE CHUNK INVENTORY
# =========================================================

datanode_inventory = {}

for address in DATANODE_ADDRESSES:
    datanode_inventory[address] = {
        "chunks": {},
        "last_update": 0,
    }


# =========================================================
# PHASE 5: DATANODE REGISTRATION HANDLER
# =========================================================

def handle_datanode_registration(request, peer_ip):
    """
    Handle DataNode registration requests.
    """
    datanode_id = request.get("datanode_id")
    ip = request.get("ip")
    port = request.get("port")
    capacity = request.get("capacity", {})
    inventory = request.get("inventory", [])
    
    if not datanode_id or not ip or port is None:
        return {
            "status": "error",
            "message": "Missing required registration fields",
        }
    
    datanode_address = (ip, port)
    
    with metadata_lock:
        # Add to dynamic registry
        dynamic_datanodes[datanode_address] = {
            "datanode_id": datanode_id,
            "ip": ip,
            "port": port,
            "capacity": capacity,
            "inventory": inventory,
            "chunk_count": len(inventory),
            "last_heartbeat": time.time(),
            "registered_at": time.time(),
        }
        
        # Initialize health tracking if not exists
        previous_status = datanode_health.get(
            datanode_address,
            {"status": "DEAD"},
        ).get("status", "DEAD")
        datanode_health[datanode_address] = {
            "status": "ALIVE",
            "last_check": time.time(),
        }
        
        # Initialize inventory if not exists
        if datanode_address not in datanode_inventory:
            datanode_inventory[datanode_address] = {
                "chunks": {},
                "last_update": 0,
            }

        update_datanode_inventory(
            datanode_address,
            inventory,
            capacity,
        )
        
        # Update display map
        if datanode_address not in DATANODE_MAP:
            DATANODE_MAP[datanode_address] = datanode_id

        if previous_status != "ALIVE":
            print(
                f"[DataNode] {datanode_id} ALIVE "
                "(registration)"
            )
    
    print(
        f"[NameNode] Registered DataNode {datanode_id} "
        f"at {ip}:{port} "
        f"(free: {capacity.get('free_bytes', 0)} bytes, "
        f"chunks: {capacity.get('chunk_count', 0)})"
    )
    publish_event(
        "datanode.registered",
        datanode_id,
        {
            "host": ip,
            "port": port,
            "chunk_count": len(inventory),
            "storage": capacity,
        },
    )
    
    return {
        "status": "success",
        "message": f"Registered {datanode_id}",
    }


# =========================================================
# THREAD SAFETY
# =========================================================

metadata_lock = threading.RLock()


# =========================================================
# PHASE 3: RE-REPLICATION STATE
# =========================================================

active_replications = set()
replication_lock = threading.Lock()


# =========================================================
# DATANODE HEALTH
# =========================================================

def mark_datanode_alive(datanode_address):
    """
    Mark a known DataNode as alive.
    """
    if datanode_address not in datanode_health:
        return False

    with metadata_lock:

        datanode_health[
            datanode_address
        ]["status"] = "ALIVE"

        datanode_health[
            datanode_address
        ]["last_check"] = time.time()

    return True

def update_datanode_inventory(
    datanode_address,
    inventory,
    capacity=None,
):
    """
    Update the NameNode's view of the chunks physically
    stored on a DataNode.
    """

    if datanode_address not in datanode_inventory:
        return False

    chunk_map = {}

    if isinstance(inventory, list):
        for item in inventory:
            if not isinstance(item, dict):
                continue

            chunk_id = item.get("chunk_id")

            if not chunk_id:
                continue

            try:
                size = int(item.get("size", 0))
            except (TypeError, ValueError):
                size = 0

            chunk_map[chunk_id] = {
                "size": size,
            }

    with metadata_lock:
        datanode_inventory[datanode_address] = {
            "chunks": chunk_map,
            "last_update": time.time(),
        }
        
        # Phase 5: Update capacity if provided
        if capacity and datanode_address in dynamic_datanodes:
            dynamic_datanodes[datanode_address]["capacity"] = capacity

        if datanode_address in dynamic_datanodes:
            dynamic_datanodes[datanode_address]["inventory"] = inventory
            dynamic_datanodes[datanode_address]["chunk_count"] = len(
                chunk_map
            )
            dynamic_datanodes[datanode_address]["last_heartbeat"] = (
                time.time()
            )

    return True
def get_inventory_consistency():
    """
    Compare NameNode metadata against the physical inventories
    reported by the DataNodes.

    Returns a diagnostic structure without modifying metadata.
    """

    result = {
        "consistent": True,
        "files": {},
    }

    with metadata_lock:

        for filename, metadata in file_metadata.items():

            expected_chunks = get_file_chunks(metadata)

            file_result = {
                "expected_chunks": len(expected_chunks),
                "chunks": {},
            }

            for chunk in expected_chunks:

                chunk_id = chunk.get("chunk_id")

                if not chunk_id:
                    continue

                expected_nodes = {
                    tuple(node)
                    for node in chunk.get(
                        "datanodes",
                        [],
                    )
                }

                actual_nodes = set()

                actual_sizes = {}

                for address, inventory in datanode_inventory.items():

                    stored_chunks = inventory.get(
                        "chunks",
                        {},
                    )

                    if chunk_id in stored_chunks:
                        actual_nodes.add(address)

                        actual_sizes[address] = (
                            stored_chunks[chunk_id].get(
                                "size",
                                0,
                            )
                        )

                missing_nodes = sorted(
                    expected_nodes - actual_nodes
                )

                unexpected_nodes = sorted(
                    actual_nodes - expected_nodes
                )

                expected_size = chunk.get(
                    "size"
                )

                size_mismatches = []

                if expected_size is not None:

                    for address, actual_size in actual_sizes.items():

                        if int(actual_size) != int(expected_size):

                            size_mismatches.append({
                                "datanode": list(address),
                                "expected_size": int(
                                    expected_size
                                ),
                                "actual_size": int(
                                    actual_size
                                ),
                            })

                chunk_consistent = (
                    len(missing_nodes) == 0
                    and len(unexpected_nodes) == 0
                    and len(size_mismatches) == 0
                )

                if not chunk_consistent:
                    result["consistent"] = False

                file_result["chunks"][chunk_id] = {
                    "expected_datanodes": [
                        list(node)
                        for node in sorted(expected_nodes)
                    ],
                    "actual_datanodes": [
                        list(node)
                        for node in sorted(actual_nodes)
                    ],
                    "missing_datanodes": [
                        list(node)
                        for node in missing_nodes
                    ],
                    "unexpected_datanodes": [
                        list(node)
                        for node in unexpected_nodes
                    ],
                    "size_mismatches": size_mismatches,
                    "consistent": chunk_consistent,
                }

            result["files"][filename] = file_result

    return result


# =========================================================
# PHASE 3: RE-REPLICATION LOGIC
# =========================================================

def find_under_replicated_chunks():
    """
    Identify chunks that have fewer replicas than the
    configured replication factor.
    """

    under_replicated = []

    with metadata_lock:

        for filename, metadata in file_metadata.items():

            chunk_list = get_file_chunks(metadata)

            for chunk in chunk_list:

                chunk_id = chunk.get("chunk_id")

                if not chunk_id:
                    continue

                datanodes = chunk.get("datanodes", [])

                alive_replicas = 0

                for address in datanodes:

                    if datanode_health.get(
                        tuple(address),
                        {"status": "DEAD"}
                    ).get("status") == "ALIVE":

                        alive_replicas += 1

                if alive_replicas < REPLICATION_FACTOR:

                    under_replicated.append({
                        "filename": filename,
                        "chunk_id": chunk_id,
                        "current_replicas": alive_replicas,
                        "required_replicas": REPLICATION_FACTOR,
                        "datanodes": datanodes,
                    })

    return under_replicated


def select_replication_target(chunk_id, source_address):
    """
    Select a target DataNode for re-replication.

    Target must be:
    - Alive
    - Not already storing this chunk
    - Not the source DataNode
    """

    alive_nodes = get_alive_datanodes()

    for address in alive_nodes:

        if address == source_address:
            continue

        inventory = datanode_inventory.get(
            address,
            {"chunks": {}}
        ).get("chunks", {})

        if chunk_id not in inventory:
            return address

    return None


def replicate_chunk(filename, chunk_id, source_address, target_address):
    """
    Trigger a chunk copy operation from source to target DataNode.
    """

    try:

        source_ip = source_address[0]
        source_port = source_address[1]
        target_ip = target_address[0]
        target_port = target_address[1]

        print(
            f"[NameNode] Re-replicating {chunk_id} "
            f"from {source_ip}:{source_port} "
            f"to {target_ip}:{target_port}"
        )

        with socket.socket(
            socket.AF_INET,
            socket.SOCK_STREAM,
        ) as source_sock:

            source_sock.connect(
                (source_ip, source_port)
            )

            send_message(
                source_sock,
                {
                    "type": "copy_chunk",
                    "chunk_id": chunk_id,
                    "destination_ip": target_ip,
                    "destination_port": target_port,
                },
            )

            response = receive_message(
                source_sock
            )

            if response and response.get(
                "status"
            ) == "success":

                print(
                    f"[NameNode] Successfully "
                    f"re-replicated {chunk_id}"
                )

                return True

            else:

                print(
                    f"[NameNode] Failed to "
                    f"re-replicate {chunk_id}: "
                    f"{response}"
                )

                return False

    except Exception as exc:

        print(
            f"[NameNode] Re-replication error "
            f"for {chunk_id}: {exc}"
        )

        return False


def update_chunk_metadata(filename, chunk_id, target_address):
    """
    Update metadata to reflect the new replica location.
    """

    with metadata_lock:

        if filename not in file_metadata:
            return False

        metadata = file_metadata[filename]
        chunk_list = get_file_chunks(metadata)

        for chunk in chunk_list:

            if chunk.get("chunk_id") == chunk_id:

                datanodes = chunk.get("datanodes", [])

                if [target_address[0], target_address[1]] not in datanodes:

                    datanodes.append(
                        [target_address[0], target_address[1]]
                    )

                    save_metadata(file_metadata)

                    print(
                        f"[NameNode] Updated metadata "
                        f"for {chunk_id} with new replica "
                        f"at {target_address}"
                    )

                    return True

        return False


def re_replication_worker():
    """
    Background worker that performs re-replication.
    """

    while True:

        time.sleep(REPLICATION_CHECK_INTERVAL)

        under_replicated = find_under_replicated_chunks()

        if not under_replicated:
            continue

        print(
            f"[NameNode] Found {len(under_replicated)} "
            f"under-replicated chunk(s)"
        )

        with replication_lock:

            for item in under_replicated:

                chunk_id = item["chunk_id"]
                filename = item["filename"]
                datanodes = item["datanodes"]

                if chunk_id in active_replications:
                    continue

                if len(active_replications) >= MAX_CONCURRENT_REPLICATIONS:
                    break

                source_address = None
                source_display = None

                for address in datanodes:

                    if datanode_health.get(
                        tuple(address),
                        {"status": "DEAD"}
                    ).get("status") == "ALIVE":

                        source_address = tuple(address)
                        source_display = DATANODE_MAP.get(
                            source_address,
                            f"{source_address[0]}:{source_address[1]}"
                        )
                        break

                if not source_address:
                    print(
                        f"[NameNode] No alive source for "
                        f"{chunk_id} (all {len(datanodes)} replicas are on DEAD nodes). "
                        f"Will retry on next check."
                    )
                    publish_event(
                        "replication.failed",
                        payload={
                            "filename": filename,
                            "chunk_id": chunk_id,
                            "reason": "NO_ALIVE_SOURCE",
                        },
                    )
                    continue

                target_address = select_replication_target(
                    chunk_id,
                    source_address
                )

                if not target_address:
                    print(
                        f"[NameNode] No available target for "
                        f"{chunk_id} from {source_display}. "
                        f"All alive nodes already have this chunk."
                    )
                    publish_event(
                        "replication.failed",
                        payload={
                            "filename": filename,
                            "chunk_id": chunk_id,
                            "reason": "NO_AVAILABLE_TARGET",
                        },
                    )
                    continue

                target_display = DATANODE_MAP.get(
                    target_address,
                    f"{target_address[0]}:{target_address[1]}"
                )

                active_replications.add(chunk_id)
                publish_event(
                    "replication.started",
                    payload={
                        "filename": filename,
                        "chunk_id": chunk_id,
                        "source": list(source_address),
                        "target": list(target_address),
                    },
                )

                threading.Thread(
                    target=lambda: re_replicate_chunk_task(
                        filename,
                        chunk_id,
                        source_address,
                        target_address
                    ),
                    daemon=True,
                ).start()


def re_replicate_chunk_task(
    filename,
    chunk_id,
    source_address,
    target_address,
):
    """
    Execute a single re-replication task.
    """

    try:

        success = replicate_chunk(
            filename,
            chunk_id,
            source_address,
            target_address
        )

        if success:

            update_chunk_metadata(
                filename,
                chunk_id,
                target_address
            )
            publish_event(
                "replication.completed",
                payload={
                    "filename": filename,
                    "chunk_id": chunk_id,
                    "target": list(target_address),
                },
            )
        else:
            publish_event(
                "replication.failed",
                payload={
                    "filename": filename,
                    "chunk_id": chunk_id,
                    "reason": "COPY_FAILED",
                },
            )

    finally:

        with replication_lock:

            active_replications.discard(chunk_id)

def get_alive_datanodes():
    """
    Return DataNodes currently marked as alive.
    """

    with metadata_lock:

        return [
            address
            for address, health
            in datanode_health.items()
            if health["status"] == "ALIVE"
        ]


# =========================================================
# METADATA FORMAT HELPERS
# =========================================================

def is_legacy_metadata(metadata):
    """
    Determine whether a file's metadata is using the
    Phase 1.1 legacy format.

    Legacy format:

        filename -> [
            {
                chunk_id,
                datanodes
            }
        ]

    Enhanced format:

        filename -> {
            filename,
            file_size,
            chunk_size,
            chunk_count,
            replication_factor,
            created_at,
            chunks
        }
    """

    return isinstance(
        metadata,
        list,
    )


def get_file_chunks(metadata):
    """
    Return the chunk list regardless of whether the metadata
    uses the legacy or enhanced format.
    """

    if is_legacy_metadata(metadata):

        return metadata

    return metadata.get(
        "chunks",
        [],
    )


# =========================================================
# CLIENT REQUEST HANDLER
# =========================================================

def handle_client_request(
    conn,
    request,
):
    """
    Handle Client requests.

    Supported:

        upload_file
        download_file
        list_files
        get_file_info
    """

    global file_metadata

    try:

        request_type = request.get(
            "type"
        )

        filename = request.get(
            "filename"
        )

        # ====================================================
        # UPLOAD FILE
        # ====================================================

        if request_type == "upload_file":

            if not filename:

                send_message(
                    conn,
                    {
                        "status": "error",
                        "message": (
                            "Filename is required."
                        ),
                    },
                )

                return

            # ------------------------------------------------
            # Phase 1.2 enhanced upload information.
            # ------------------------------------------------

            enhanced_chunks = request.get(
                "chunks"
            )

            file_size = request.get(
                "file_size"
            )

            requested_chunk_size = request.get(
                "chunk_size"
            )

            num_chunks = request.get(
                "num_chunks"
            )

            # ------------------------------------------------
            # Backward compatibility with Phase 1.1 client.
            # ------------------------------------------------

            if not enhanced_chunks:

                chunk_ids = request.get(
                    "chunk_ids",
                    [],
                )

                if not chunk_ids:

                    send_message(
                        conn,
                        {
                            "status": "error",
                            "message": (
                                "No chunks were provided."
                            ),
                        },
                    )

                    return

                enhanced_chunks = []

                for index, chunk_id in enumerate(
                    chunk_ids
                ):

                    enhanced_chunks.append(
                        {
                            "chunk_id": chunk_id,
                            "chunk_index": index,
                            "size": 0,
                        }
                    )

                if num_chunks is None:

                    num_chunks = len(
                        enhanced_chunks
                    )

            # ------------------------------------------------
            # Validate chunk information.
            # ------------------------------------------------

            if not isinstance(
                enhanced_chunks,
                list,
            ) or not enhanced_chunks:

                send_message(
                    conn,
                    {
                        "status": "error",
                        "message": (
                            "Invalid chunk metadata."
                        ),
                    },
                )

                return

            if num_chunks is None:

                num_chunks = len(
                    enhanced_chunks
                )

            if num_chunks != len(
                enhanced_chunks
            ):

                send_message(
                    conn,
                    {
                        "status": "error",
                        "message": (
                            "Chunk count does not match "
                            "chunk metadata."
                        ),
                    },
                )

                return

            # ------------------------------------------------
            # Use configured chunk size when the Client does
            # not provide one.
            # ------------------------------------------------

            if requested_chunk_size is None:

                requested_chunk_size = (
                    CHUNK_SIZE_BYTES
                )

            # ------------------------------------------------
            # Validate file size.
            # ------------------------------------------------

            if file_size is None:

                calculated_size = 0

                for chunk in enhanced_chunks:

                    try:
                        calculated_size += int(
                            chunk.get(
                                "size",
                                0,
                            )
                        )

                    except (
                        TypeError,
                        ValueError,
                    ):
                        pass

                file_size = calculated_size

            # =================================================
            # Check DataNode availability
            # =================================================

            available_nodes = (
                get_alive_datanodes()
            )

            if not available_nodes:

                send_message(
                    conn,
                    {
                        "status": "error",
                        "message": (
                            "Not enough Datanodes alive "
                            "for replication. "
                            "Required: at least 1, "
                            f"Available: "
                            f"{len(available_nodes)}."
                        ),
                    },
                )

                return

            # =================================================
            # Select DataNodes and build metadata
            # =================================================

            response_chunks = []
            stored_chunks = []

            for index, chunk in enumerate(
                enhanced_chunks
            ):

                chunk_id = chunk.get(
                    "chunk_id"
                )

                if not chunk_id:

                    send_message(
                        conn,
                        {
                            "status": "error",
                            "message": (
                                f"Missing chunk_id "
                                f"for chunk {index}."
                            ),
                        },
                    )

                    return

                chunk_index = chunk.get(
                    "chunk_index",
                    index,
                )

                try:

                    chunk_index = int(
                        chunk_index
                    )

                except (
                    TypeError,
                    ValueError,
                ):

                    chunk_index = index

                chunk_size = chunk.get(
                    "size",
                    0,
                )

                try:

                    chunk_size = int(
                        chunk_size
                    )

                except (
                    TypeError,
                    ValueError,
                ):

                    chunk_size = 0

                replica_count = min(
                    REPLICATION_FACTOR,
                    len(available_nodes),
                )
                selected_datanodes = []
                if DATANODE3_ADDRESS in available_nodes:
                    selected_datanodes.append(DATANODE3_ADDRESS)
                remaining_nodes = [
                    address for address in available_nodes
                    if address not in selected_datanodes
                ]
                selected_datanodes.extend(
                    random.sample(
                        remaining_nodes,
                        min(
                            replica_count - len(selected_datanodes),
                            len(remaining_nodes),
                        ),
                    )
                )

                datanode_ports = [
                    address[1]
                    for address
                    in selected_datanodes
                ]

                # Log chunk distribution
                datanode_names = [
                    DATANODE_MAP.get(addr, f"{addr[0]}:{addr[1]}")
                    for addr in selected_datanodes
                ]
                print(
                    f"[NameNode] Allocating {chunk_id} (chunk {chunk_index}) "
                    f"to DataNodes: {', '.join(datanode_names)}"
                )

                response_chunks.append(
                    {
                        "chunk_id": chunk_id,
                        "chunk_index": chunk_index,
                        "size": chunk_size,
                        "datanodes": datanode_ports,
                        "desired_replication": REPLICATION_FACTOR,
                    }
                )

                stored_chunks.append(
                    {
                        "chunk_id": chunk_id,
                        "chunk_index": chunk_index,
                        "size": chunk_size,
                        "datanodes": [
                            [
                                address[0],
                                address[1],
                            ]
                            for address
                            in selected_datanodes
                        ],
                    }
                )

            # =================================================
            # Create enhanced file metadata
            # =================================================

            created_at = datetime.now(
                timezone.utc
            ).isoformat()

            enhanced_metadata = {
                "filename": filename,
                "file_size": int(file_size),
                "chunk_size": int(
                    requested_chunk_size
                ),
                "chunk_count": len(
                    stored_chunks
                ),
                "replication_factor": (
                    REPLICATION_FACTOR
                ),
                "created_at": created_at,
                "chunks": stored_chunks,
            }

            # =================================================
            # Keep the placement plan pending until the client
            # confirms physical DataNode storage.
            # =================================================

            upload_id = f"upload_{time.time_ns()}"
            with metadata_lock:
                pending_uploads[upload_id] = enhanced_metadata

            print(
                f"[NameNode] Placement plan created for "
                f"{filename} ({upload_id})"
            )

            print(
                f"[NameNode] Chunks: "
                f"{len(stored_chunks)}"
            )

            print(
                f"[NameNode] Chunk size: "
                f"{requested_chunk_size} bytes"
            )

            print(
                f"[NameNode] Replication: "
                f"{REPLICATION_FACTOR}"
            )

            send_message(
                conn,
                {
                    "status": "success",
                    "upload_id": upload_id,
                    "chunks": response_chunks,
                },
            )

        # ====================================================
        # FINALIZE UPLOAD
        # ====================================================

        elif request_type == "finalize_upload":

            upload_id = request.get("upload_id")
            confirmed_chunks = request.get("chunks", [])

            with metadata_lock:
                pending_metadata = pending_uploads.pop(upload_id, None)

            if pending_metadata is None:
                send_message(
                    conn,
                    {
                        "status": "error",
                        "message": "Upload plan not found or expired.",
                    },
                )
                return

            confirmed_by_id = {
                item.get("chunk_id"): item
                for item in confirmed_chunks
                if isinstance(item, dict) and item.get("chunk_id")
            }
            under_replicated = []

            for chunk in pending_metadata["chunks"]:
                confirmation = confirmed_by_id.get(chunk["chunk_id"], {})
                planned_nodes = {
                    tuple(node)
                    for node in chunk.get("datanodes", [])
                }
                actual_nodes = []
                for port in confirmation.get("datanodes", []):
                    matching = [
                        node for node in planned_nodes
                        if node[1] == port
                    ]
                    if matching and list(matching[0]) not in actual_nodes:
                        actual_nodes.append(list(matching[0]))

                if not actual_nodes:
                    send_message(
                        conn,
                        {
                            "status": "error",
                            "message": (
                                f"No confirmed replica for {chunk['chunk_id']}."
                            ),
                        },
                    )
                    return

                chunk["datanodes"] = actual_nodes
                chunk["actual_replication"] = len(actual_nodes)
                chunk["desired_replication"] = REPLICATION_FACTOR
                checksum = confirmation.get("checksum")
                if checksum:
                    chunk["checksum"] = checksum
                if len(actual_nodes) < REPLICATION_FACTOR:
                    under_replicated.append(chunk["chunk_id"])

            pending_metadata["replication_status"] = (
                "UNDER_REPLICATED" if under_replicated else "HEALTHY"
            )

            with metadata_lock:
                file_metadata[pending_metadata["filename"]] = pending_metadata
                save_metadata(file_metadata)

            publish_event(
                "chunk.created",
                payload={
                    "filename": pending_metadata["filename"],
                    "chunks": [
                        {
                            "chunk_id": chunk["chunk_id"],
                            "chunk_index": chunk.get("chunk_index"),
                            "replicas": chunk.get("datanodes", []),
                            "checksum": chunk.get("checksum"),
                        }
                        for chunk in pending_metadata["chunks"]
                    ],
                    "replication_status": pending_metadata[
                        "replication_status"
                    ],
                },
            )

            send_message(
                conn,
                {
                    "status": (
                        "partial_success" if under_replicated else "success"
                    ),
                    "filename": pending_metadata["filename"],
                    "under_replicated_chunks": under_replicated,
                },
            )

        # ====================================================
        # DOWNLOAD FILE
        # ====================================================

        elif request_type == "download_file":

            with metadata_lock:

                metadata = file_metadata.get(
                    filename
                )

            if metadata is None:

                send_message(
                    conn,
                    {
                        "status": "error",
                        "message": "File not found.",
                    },
                )

                return

            chunk_list = get_file_chunks(
                metadata
            )

            chunk_map = []

            for chunk in chunk_list:

                chunk_map.append(
                    {
                        "chunk_id": chunk.get(
                            "chunk_id"
                        ),
                        "datanodes": [
                            list(address)
                            for address in chunk.get(
                                "datanodes",
                                [],
                            )
                        ],
                    }
                )

            # ------------------------------------------------
            # Sort enhanced metadata by chunk index.
            # ------------------------------------------------

            if not is_legacy_metadata(
                metadata
            ):

                chunk_map = [
                    {
                        "chunk_id": chunk.get(
                            "chunk_id"
                        ),
                        "datanodes": [
                            list(address)
                            for address in chunk.get(
                                "datanodes",
                                [],
                            )
                        ],
                    }
                    for chunk in sorted(
                        chunk_list,
                        key=lambda item: item.get(
                            "chunk_index",
                            0,
                        ),
                    )
                ]

            send_message(
                conn,
                {
                    "status": "success",
                    "chunks": chunk_map,
                },
            )

        # ====================================================
        # LIST FILES
        # ====================================================

        elif request_type == "list_files":

            with metadata_lock:

                files = list(
                    file_metadata.keys()
                )

            send_message(
                conn,
                {
                    "status": "success",
                    "files": files,
                },
            )

        # ====================================================
        # SYSTEM STATUS
        # ====================================================

        elif request_type == "system_status":

            with metadata_lock:

                # Collect DataNode health information
                datanode_info = []
                for address, health in datanode_health.items():

                    display_name = DATANODE_MAP.get(
                        tuple(address),
                        f"DN-{address[1]}",
                    )

                    inventory = datanode_inventory.get(
                        address,
                        {"chunks": {}}
                    )

                    capacity = {}
                    if address in dynamic_datanodes:
                        capacity = dynamic_datanodes[
                            address
                        ].get("capacity", {})

                    datanode_info.append({
                        "id": display_name,
                        "address": f"{address[0]}:{address[1]}",
                        "status": health["status"],
                        "last_check": health["last_check"],
                        "chunk_count": len(inventory.get("chunks", {})),
                        "capacity": capacity,
                    })

                # Collect file statistics
                file_count = 0
                total_chunks = 0
                total_size = 0

                for path, metadata in file_metadata.items():

                    if metadata.get("type") == "directory":
                        continue

                    file_count += 1

                    if not is_legacy_metadata(metadata):
                        total_chunks += metadata.get(
                            "chunk_count",
                            0
                        )
                        total_size += metadata.get(
                            "file_size",
                            0
                        )
                    else:
                        total_chunks += len(metadata)

                # Calculate system health
                alive_datanodes = sum(
                    1 for dn in datanode_info
                    if dn["status"] == "ALIVE"
                )

                system_health = "healthy"
                if alive_datanodes < REPLICATION_FACTOR:
                    system_health = "degraded"
                if alive_datanodes == 0:
                    system_health = "critical"

            send_message(
                conn,
                {
                    "status": "success",
                    "system": {
                        "health": system_health,
                        "namenode": "online",
                        "replication_factor": REPLICATION_FACTOR,
                        "chunk_size_bytes": CHUNK_SIZE_BYTES,
                    },
                    "datanodes": datanode_info,
                    "statistics": {
                        "total_files": file_count,
                        "total_directories": len(file_metadata) - file_count,
                        "total_chunks": total_chunks,
                        "total_storage_bytes": total_size,
                        "alive_datanodes": alive_datanodes,
                        "total_datanodes": len(datanode_info),
                    },
                },
            )

        # ====================================================
        # FILE INFORMATION
        # ====================================================

        elif request_type == "get_file_info":

            with metadata_lock:

                metadata = file_metadata.get(
                    filename
                )

                if metadata is None:

                    send_message(
                        conn,
                        {
                            "status": "error",
                            "message": (
                                "File not found."
                            ),
                        },
                    )

                    return

                chunk_list = get_file_chunks(
                    metadata
                )

                detailed_chunks = []

                for chunk in chunk_list:

                    chunk_datanodes = []
                    datanode_status = {}

                    for address in chunk.get(
                        "datanodes",
                        [],
                    ):

                        display_name = (
                            DATANODE_MAP.get(
                                tuple(address),
                                f"DN-{address[1]}",
                            )
                        )

                        status = (
                            datanode_health.get(
                                tuple(address),
                                {
                                    "status": "DEAD"
                                },
                            )
                            .get(
                                "status",
                                "DEAD",
                            )
                            .lower()
                        )

                        chunk_datanodes.append(
                            display_name
                        )

                        datanode_status[
                            display_name
                        ] = status

                    chunk_info = {
                        "chunk_id": chunk.get(
                            "chunk_id"
                        ),
                        "datanodes": (
                            chunk_datanodes
                        ),
                        "datanode_status": (
                            datanode_status
                        ),
                    }

                    # Add enhanced metadata fields.
                    if not is_legacy_metadata(
                        metadata
                    ):

                        chunk_info[
                            "chunk_index"
                        ] = chunk.get(
                            "chunk_index",
                            0,
                        )

                        chunk_info[
                            "size"
                        ] = chunk.get(
                            "size",
                            0,
                        )

                    detailed_chunks.append(
                        chunk_info
                    )

                response = {
                    "status": "success",
                    "filename": filename,
                    "chunks": detailed_chunks,
                }

                # Add enhanced file information.
                if not is_legacy_metadata(
                    metadata
                ):

                    response.update(
                        {
                            "file_size": metadata.get(
                                "file_size",
                                0,
                            ),
                            "chunk_size": metadata.get(
                                "chunk_size",
                                CHUNK_SIZE_BYTES,
                            ),
                            "chunk_count": metadata.get(
                                "chunk_count",
                                len(chunk_list),
                            ),
                            "replication_factor": (
                                metadata.get(
                                    "replication_factor",
                                    REPLICATION_FACTOR,
                                )
                            ),
                            "created_at": metadata.get(
                                "created_at"
                            ),
                        }
                    )

            send_message(
                conn,
                response,
            )

        # ====================================================
        # DELETE FILE
        # ====================================================

        elif request_type == "delete_file":

            with metadata_lock:

                metadata = file_metadata.get(
                    filename
                )

            if metadata is None:

                send_message(
                    conn,
                    {
                        "status": "error",
                        "message": "File not found.",
                    },
                )

                return

            chunk_list = get_file_chunks(
                metadata
            )

            # Collect all unique chunk IDs and their DataNode locations
            chunks_to_delete = {}
            for chunk in chunk_list:
                chunk_id = chunk.get("chunk_id")
                datanodes = chunk.get("datanodes", [])
                chunks_to_delete[chunk_id] = datanodes

            # Delete chunks from DataNodes
            deleted_count = 0
            failed_deletions = []
            for chunk_id, datanodes in chunks_to_delete.items():
                for datanode_address in datanodes:
                    try:
                        datanode_ip = datanode_address[0]
                        datanode_port = datanode_address[1]

                        with socket.socket(
                            socket.AF_INET,
                            socket.SOCK_STREAM,
                        ) as dn_sock:
                            dn_sock.connect(
                                (datanode_ip, datanode_port)
                            )
                            send_message(
                                dn_sock,
                                {
                                    "type": "delete_chunk",
                                    "chunk_id": chunk_id,
                                },
                            )
                            response = receive_message(dn_sock)
                            if response and response.get("status") == "success":
                                deleted_count += 1
                                print(
                                    f"[NameNode] Deleted {chunk_id} "
                                    f"from {datanode_ip}:{datanode_port}"
                                )
                            elif response and "not found" in response.get(
                                "message", ""
                            ).lower():
                                deleted_count += 1
                            else:
                                failed_deletions.append(
                                    {
                                        "chunk_id": chunk_id,
                                        "datanode": list(datanode_address),
                                    }
                                )
                    except Exception as exc:
                        failed_deletions.append(
                            {
                                "chunk_id": chunk_id,
                                "datanode": list(datanode_address),
                                "error": str(exc),
                            }
                        )
                        print(
                            f"[NameNode] Failed to delete {chunk_id} "
                            f"from {datanode_address}: {exc}"
                        )

            # Remove metadata only after all physical replicas are cleaned.
            with metadata_lock:
                if failed_deletions:
                    metadata["deletion_state"] = "PENDING_CLEANUP"
                    metadata["pending_deletions"] = failed_deletions
                else:
                    del file_metadata[filename]
                save_metadata(file_metadata)

            publish_event(
                "file.deleted" if not failed_deletions else "file.delete_pending",
                payload={
                    "filename": filename,
                    "chunks_deleted": deleted_count,
                    "pending_deletions": failed_deletions,
                },
            )

            send_message(
                conn,
                {
                    "status": (
                        "partial_success" if failed_deletions else "success"
                    ),
                    "message": (
                        f"Deleted {filename} with pending cleanup"
                        if failed_deletions
                        else f"Deleted {filename}"
                    ),
                    "chunks_deleted": deleted_count,
                    "pending_deletions": failed_deletions,
                },
            )

        # ====================================================
        # RENAME FILE
        # ====================================================

        elif request_type == "rename_file":

            new_filename = request.get("new_filename")

            if not new_filename:

                send_message(
                    conn,
                    {
                        "status": "error",
                        "message": "new_filename is required.",
                    },
                )

                return

            with metadata_lock:

                if filename not in file_metadata:

                    send_message(
                        conn,
                        {
                            "status": "error",
                            "message": "File not found.",
                        },
                    )

                    return

                if new_filename in file_metadata:

                    send_message(
                        conn,
                        {
                            "status": "error",
                            "message": (
                                "Target filename already exists."
                            ),
                        },
                    )

                    return

                # Update metadata with new filename
                metadata = file_metadata[filename]
                metadata["filename"] = new_filename
                file_metadata[new_filename] = metadata
                del file_metadata[filename]
                save_metadata(file_metadata)

            send_message(
                conn,
                {
                    "status": "success",
                    "message": f"Renamed {filename} to {new_filename}",
                },
            )

        # ====================================================
        # DATA CONSISTENCY AUDIT
        # ====================================================

        elif request_type == "audit_consistency":

            consistency_issues = get_inventory_consistency()
            issue_count = sum(
                1
                for file_result in consistency_issues["files"].values()
                for chunk_result in file_result["chunks"].values()
                if not chunk_result["consistent"]
            )

            send_message(
                conn,
                {
                    "status": "success",
                    "issues": consistency_issues,
                    "issue_count": issue_count,
                },
            )

            print(
                f"[NameNode] Consistency audit completed: "
                f"{issue_count} issues found"
            )

        # ====================================================
        # CLUSTER STATUS
        # ====================================================

        elif request_type == "cluster_status":

            alive_nodes = get_alive_datanodes()
            total_nodes = len(datanode_health)

            # Count total chunks
            total_chunks = 0
            for filename, metadata in file_metadata.items():
                chunks = get_file_chunks(metadata)
                total_chunks += len(chunks)

            # Count under-replicated chunks
            under_replicated = 0
            for filename, metadata in file_metadata.items():
                chunks = get_file_chunks(metadata)
                for chunk in chunks:
                    datanodes = chunk.get("datanodes", [])
                    alive_replicas = 0
                    for addr in datanodes:
                        if datanode_health.get(tuple(addr), {}).get("status") == "ALIVE":
                            alive_replicas += 1
                    if alive_replicas < REPLICATION_FACTOR:
                        under_replicated += 1

            send_message(
                conn,
                {
                    "status": "success",
                    "cluster": {
                        "total_datanodes": total_nodes,
                        "alive_datanodes": len(alive_nodes),
                        "dead_datanodes": total_nodes - len(alive_nodes),
                        "total_files": len(file_metadata),
                        "total_chunks": total_chunks,
                        "under_replicated_chunks": under_replicated,
                        "replication_factor": REPLICATION_FACTOR,
                    },
                    "datanodes": [
                        {
                            "address": f"{addr[0]}:{addr[1]}",
                            "datanode_id": dynamic_datanodes.get(
                                addr,
                                {},
                            ).get(
                                "datanode_id",
                                DATANODE_MAP.get(addr, f"DN-{addr[1]}"),
                            ),
                            "status": health.get("status", "UNKNOWN"),
                            "last_heartbeat": health.get("last_check", 0),
                            "chunk_count": len(
                                datanode_inventory.get(
                                    addr,
                                    {"chunks": {}},
                                ).get("chunks", {})
                            ),
                            "capacity": dynamic_datanodes.get(
                                addr,
                                {},
                            ).get(
                                "capacity",
                                health.get("capacity", {}),
                            ),
                        }
                        for addr, health in datanode_health.items()
                    ],
                },
            )

        # ====================================================
        # CREATE DIRECTORY
        # ====================================================

        elif request_type == "mkdir":

            dirname = request.get("dirname")

            if not dirname:

                send_message(
                    conn,
                    {
                        "status": "error",
                        "message": "dirname is required.",
                    },
                )

                return

            # Normalize directory path (ensure it ends with /)
            if not dirname.endswith("/"):
                dirname += "/"

            with metadata_lock:

                # Check if directory already exists
                if dirname in file_metadata:

                    send_message(
                        conn,
                        {
                            "status": "error",
                            "message": "Directory already exists.",
                        },
                    )

                    return

                # Check if any file/directory with this prefix exists
                for existing_path in file_metadata.keys():
                    if existing_path.startswith(dirname):
                        send_message(
                            conn,
                            {
                                "status": "error",
                                "message": (
                                    "Cannot create directory: "
                                    "path conflicts with existing files."
                                ),
                            },
                        )
                        return

                # Create directory metadata
                dir_metadata = {
                    "type": "directory",
                    "path": dirname,
                    "created_at": datetime.now(
                        timezone.utc
                    ).isoformat(),
                }

                file_metadata[dirname] = dir_metadata
                save_metadata(file_metadata)

            send_message(
                conn,
                {
                    "status": "success",
                    "message": f"Created directory {dirname}",
                },
            )

        # ====================================================
        # LIST DIRECTORY
        # ====================================================

        elif request_type == "list_directory":

            dirname = request.get("dirname", "")

            # Normalize directory path
            if dirname and not dirname.endswith("/"):
                dirname += "/"

            with metadata_lock:

                contents = []

                for path, metadata in file_metadata.items():

                    # Skip if metadata is not a dict (legacy format)
                    if not isinstance(metadata, dict):
                        continue

                    # Check if this item is in the requested directory
                    if dirname:

                        if path.startswith(dirname):

                            # Get the relative path (remove directory prefix)
                            relative_path = path[len(dirname):]

                            # Only include direct children (no nested paths)
                            if "/" not in relative_path or (
                                metadata.get("type") == "directory" and
                                relative_path.endswith("/")
                            ):
                                contents.append({
                                    "name": path,
                                    "type": metadata.get(
                                        "type",
                                        "file"
                                    ),
                                })

                    else:

                        # Root directory - include all top-level items
                        if "/" not in path or path.endswith("/"):
                            contents.append({
                                "name": path,
                                "type": metadata.get(
                                    "type",
                                    "file"
                                ),
                            })

            send_message(
                conn,
                {
                    "status": "success",
                    "directory": dirname if dirname else "/",
                    "contents": contents,
                },
            )

        # ====================================================
        # DELETE DIRECTORY
        # ====================================================

        elif request_type == "rmdir":

            dirname = request.get("dirname")

            if not dirname:

                send_message(
                    conn,
                    {
                        "status": "error",
                        "message": "dirname is required.",
                    },
                )

                return

            # Normalize directory path
            if not dirname.endswith("/"):
                dirname += "/"

            with metadata_lock:

                if dirname not in file_metadata:

                    send_message(
                        conn,
                        {
                            "status": "error",
                            "message": "Directory not found.",
                        },
                    )

                    return

                if file_metadata[dirname].get("type") != "directory":

                    send_message(
                        conn,
                        {
                            "status": "error",
                            "message": "Path is not a directory.",
                        },
                    )

                    return

                # Check if directory is empty
                has_contents = False
                for path in file_metadata.keys():
                    if path.startswith(dirname) and path != dirname:
                        has_contents = True
                        break

                if has_contents:

                    send_message(
                        conn,
                        {
                            "status": "error",
                            "message": (
                                "Directory is not empty. "
                                "Delete contents first."
                            ),
                        },
                    )

                    return

                # Delete directory metadata
                del file_metadata[dirname]
                save_metadata(file_metadata)

            send_message(
                conn,
                {
                    "status": "success",
                    "message": f"Deleted directory {dirname}",
                },
            )

        # ====================================================
        # UNKNOWN REQUEST
        # ====================================================

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
            f"[NameNode] Client request error: "
            f"{exc}"
        )

        try:

            send_message(
                conn,
                {
                    "status": "error",
                    "message": (
                        "Internal NameNode error."
                    ),
                },
            )

        except Exception:
            pass


# =========================================================
# CONNECTION DISPATCHER
# =========================================================

def handle_connection(conn):

    try:

        request = receive_message(
            conn
        )

        if not request:
            return

        request_type = request.get(
            "type"
        )

        if request_type == "heartbeat":

            datanode_port = request.get("port")
            datanode_id = request.get("datanode_id")
            
            # Find registered DataNode by port and use its registered IP
            datanode_address = None
            for addr, info in dynamic_datanodes.items():
                if info.get("port") == datanode_port and info.get("datanode_id") == datanode_id:
                    datanode_address = addr
                    break
            
            # Fallback to peer IP if not found in registry
            if not datanode_address:
                try:
                    peer_ip = conn.getpeername()[0]
                except Exception:
                    peer_ip = None
                datanode_address = (peer_ip, datanode_port)

            was_alive = datanode_health.get(
                datanode_address,
                {},
            ).get("status") == "ALIVE"

            if mark_datanode_alive(datanode_address):

                display_name = DATANODE_MAP.get(
                    datanode_address,
                    "Unknown",
                )
                inventory = request.get("inventory", [])
                capacity = request.get("capacity")
                chunk_count = len(inventory)
                free_bytes = (
                    capacity.get("free_bytes", 0)
                    if isinstance(capacity, dict)
                    else 0
                )
                previous_count = len(
                    datanode_inventory.get(
                        datanode_address,
                        {"chunks": {}},
                    ).get("chunks", {})
                )

                if not was_alive or previous_count != chunk_count:
                    print(
                        f"[Heartbeat] {display_name} "
                        f"({datanode_id}) ALIVE: "
                        f"{chunk_count} chunks, "
                        f"{free_bytes} bytes free"
                    )
                    publish_event(
                        "heartbeat.received",
                        datanode_id,
                        {
                            "host": datanode_address[0],
                            "port": datanode_address[1],
                            "status": "ALIVE",
                            "chunk_count": chunk_count,
                            "storage_used": (
                                capacity.get("used_bytes", 0)
                                if isinstance(capacity, dict)
                                else 0
                            ),
                            "storage_free": free_bytes,
                        },
                    )

                update_datanode_inventory(
                    datanode_address,
                    inventory,
                    capacity,
                )
                with metadata_lock:
                    datanode_health[datanode_address]["capacity"] = (
                        capacity if isinstance(capacity, dict) else {}
                    )

            else:

                print(
                    f"[NameNode] Heartbeat from "
                    f"unregistered DataNode {datanode_id} "
                    f"at {datanode_address[0]}:{datanode_address[1]}"
                )
        # Phase 5: DATA NODE REGISTRATION
        # ====================================================

        elif request_type == "register_datanode":

            try:
                peer_ip = conn.getpeername()[0]
            except Exception:
                peer_ip = None

            response = handle_datanode_registration(
                request,
                peer_ip,
            )

            send_message(
                conn,
                response,
            )

        else:

            handle_client_request(
                conn,
                request,
            )

    except Exception as exc:

        print(
            f"[NameNode] Connection handling error: "
            f"{exc}"
        )

    finally:

        try:
            conn.close()

        except Exception:
            pass


# =========================================================
# DATANODE HEALTH MONITOR
# =========================================================

def check_datanode_health():

    check_interval = (
        HEARTBEAT_INTERVAL * 2
    )

    timeout = HEARTBEAT_TIMEOUT

    while True:

        time.sleep(
            check_interval
        )

        current_time = time.time()

        with metadata_lock:

            for address, health in (
                datanode_health.items()
            ):

                if health["status"] != "ALIVE":
                    continue

                elapsed = (
                    current_time
                    - health["last_check"]
                )

                if elapsed > timeout:

                    health["status"] = "DEAD"

                    display_name = (
                        DATANODE_MAP.get(
                            address,
                            "Unknown",
                        )
                    )

                    print(
                        f"[NameNode] DataNode "
                        f"{display_name} at {address[0]}:{address[1]} "
                        f"timed out (last heartbeat {elapsed:.1f}s ago). "
                        f"Status: ALIVE → DEAD"
                    )
                    publish_event(
                        "datanode.failed",
                        DATANODE_MAP.get(address, "Unknown"),
                        {
                            "host": address[0],
                            "port": address[1],
                            "last_heartbeat": health["last_check"],
                            "reason": "HEARTBEAT_TIMEOUT",
                        },
                    )


# =========================================================
# NAMENODE SERVER
# =========================================================

def start_namenode():

    server_socket = socket.socket(
        socket.AF_INET,
        socket.SOCK_STREAM,
    )

    server_socket.setsockopt(
        socket.SOL_SOCKET,
        socket.SO_REUSEADDR,
        1,
    )

    server_socket.bind(
    (NAMENODE_BIND_IP, NAMENODE_PORT)
    )

    server_socket.listen(10)

    print("=" * 60)
    print("HDFS NameNode")
    print("=" * 60)

    print(
        f"Listening on "
        f"{NAMENODE_BIND_IP}:{NAMENODE_PORT}"
    )

    print(
        f"Listening on "
        f"{NAMENODE_ADDRESS[0]}:"
        f"{NAMENODE_ADDRESS[1]}"
    )

    print(
        f"Configured DataNodes: "
        f"{len(DATANODE_ADDRESSES)}"
    )

    print(
        f"Replication factor: "
        f"{REPLICATION_FACTOR}"
    )

    print(
        f"Heartbeat interval: "
        f"{HEARTBEAT_INTERVAL}s"
    )

    print(
        f"Persisted files: "
        f"{len(file_metadata)}"
    )

    print("=" * 60)

    # Phase 17: Startup recovery - wait a bit for DataNodes to register
    print("[NameNode] Waiting for DataNodes to register...")
    time.sleep(10)  # Give DataNodes time to register

    # Trigger inventory consistency check
    consistency = get_inventory_consistency()
    if consistency:
        print(f"[NameNode] Inventory consistency check: {len(consistency)} issues found")
        for issue in consistency:
            print(f"  - {issue}")
    else:
        print("[NameNode] Inventory consistency check: OK")

    threading.Thread(
        target=check_datanode_health,
        daemon=True,
        name="DataNodeHealthMonitor",
    ).start()

    threading.Thread(
        target=re_replication_worker,
        daemon=True,
        name="ReReplicationWorker",
    ).start()

    while True:

        try:

            conn, address = (
                server_socket.accept()
            )

            threading.Thread(
                target=handle_connection,
                args=(conn,),
                daemon=True,
            ).start()

        except KeyboardInterrupt:

            print(
                "\n[NameNode] Shutdown requested."
            )

            break

        except Exception as exc:

            print(
                f"[NameNode] Server error: "
                f"{exc}"
            )

    server_socket.close()


# =========================================================
# MAIN
# =========================================================

if __name__ == "__main__":
    start_namenode()