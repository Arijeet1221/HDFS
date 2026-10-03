# MINI_HDFS/client/client.py

import os
import sys
import socket
import uuid
import json
import struct
import threading
from collections import deque
from flask import Flask, request, jsonify, render_template, send_file


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
    NAMENODE_IP,
    NAMENODE_PORT,
    DATANODE_PORT_TO_IP,
    CLIENT_IP,
    CLIENT_PORT,
    CHUNK_SIZE_BYTES,
)

from common.utils import (
    send_message,
    receive_message,
    receive_file,
    recv_exact,
    verify_checksum,
)
from common.kafka_consumer import KafkaEventConsumer


def receive_chunk_response(sock):
    """Read current JSON and legacy size-prefixed chunk replies."""
    header = recv_exact(sock, 4)
    value = struct.unpack(">I", header)[0]

    if value > 1024 * 1024:
        return {
            "status": "success",
            "file_size": value,
            "checksum": None,
            "legacy_payload": recv_exact(sock, value),
        }

    payload = recv_exact(sock, value)
    response = json.loads(payload.decode("utf-8"))
    response["legacy_payload"] = None
    return response


# =========================================================
# FLASK APPLICATION
# =========================================================

app = Flask(
    __name__,
    template_folder="templates",
)


app.config["UPLOAD_FOLDER"] = os.path.join(
    PROJECT_ROOT,
    "uploads",
)

event_history = deque(maxlen=100)
event_history_lock = threading.Lock()


def record_kafka_event(event):
    with event_history_lock:
        event_history.appendleft(event)


def start_kafka_event_consumer():
    consumer = KafkaEventConsumer()
    consumer.consume_forever(record_kafka_event)


os.makedirs(
    app.config["UPLOAD_FOLDER"],
    exist_ok=True,
)


# =========================================================
# HDFS CLIENT
# =========================================================

class HDFSClient:

    def __init__(
        self,
        namenode_host=NAMENODE_IP,
    ):

        self.namenode_host = namenode_host
        self.namenode_port = NAMENODE_PORT

    # =====================================================
    # CHUNK ID
    # =====================================================

    def _generate_chunk_id(self, filename, chunk_index):
        """
        Generate a unique chunk ID.
        """
        return f"{filename}_{chunk_index}_{uuid.uuid4().hex}"

    def _generate_file_id(self):
        """
        Generate a unique file ID.
        """
        return f"file_{uuid.uuid4().hex}"

    # =====================================================
    # UPLOAD
    # =====================================================

    def upload_file(self, filepath):
        """
        Upload a file to HDFS.

        Phase 1.2:
        The NameNode now receives enhanced file/chunk
        metadata including:

            - file size
            - chunk size
            - chunk count
            - chunk index
            - individual chunk size
            - chunk ID
        """

        try:

            filename = os.path.basename(
                filepath
            )

            # ------------------------------------------------
            # Read and split the file into chunks.
            # ------------------------------------------------

            chunks = []

            total_file_size = 0
            chunk_index = 0

            with open(
                filepath,
                "rb",
            ) as file:

                while True:

                    chunk_data = file.read(
                        CHUNK_SIZE_BYTES
                    )

                    if not chunk_data:
                        break

                    chunk_id = (
                        self._generate_chunk_id(
                            filename,
                            chunk_index,
                        )
                    )

                    chunk_size = len(
                        chunk_data
                    )

                    chunks.append(
                        {
                            "chunk_id": chunk_id,
                            "chunk_index": chunk_index,
                            "size": chunk_size,
                            "data": chunk_data,
                        }
                    )

                    total_file_size += chunk_size

                    chunk_index += 1

            # ------------------------------------------------
            # Empty file check.
            # ------------------------------------------------

            if len(chunks) == 0:

                return {
                    "status": "error",
                    "message": (
                        "Cannot upload an empty file."
                    ),
                }

            # ------------------------------------------------
            # Build enhanced chunk metadata for NameNode.
            # ------------------------------------------------

            chunk_metadata = []

            for chunk in chunks:

                chunk_metadata.append(
                    {
                        "chunk_id": chunk["chunk_id"],
                        "chunk_index": chunk["chunk_index"],
                        "size": chunk["size"],
                    }
                )

            # =================================================
            # 1. Ask NameNode for DataNode placement
            # =================================================

            namenode_sock = socket.socket(
                socket.AF_INET,
                socket.SOCK_STREAM,
            )

            try:
                namenode_sock.connect(
                    (
                        self.namenode_host,
                        self.namenode_port,
                    )
                )
            except (ConnectionRefusedError, socket.timeout, OSError) as exc:
                return {
                    "status": "error",
                    "message": f"Cannot connect to NameNode: {exc}",
                }

            try:
                send_message(
                    namenode_sock,
                    {
                        "type": "upload_file",
                        "filename": filename,
                        "file_size": total_file_size,
                        "chunk_size": CHUNK_SIZE_BYTES,
                        "num_chunks": len(chunks),
                        "chunks": chunk_metadata,
                        # Keep chunk_ids for backward compatibility.
                        "chunk_ids": [
                            chunk["chunk_id"]
                            for chunk in chunks
                        ],
                    },
                )

                response = receive_message(
                    namenode_sock
                )
            except (ConnectionError, OSError) as exc:
                namenode_sock.close()
                return {
                    "status": "error",
                    "message": f"Communication error with NameNode: {exc}",
                }

            namenode_sock.close()

            if not response:

                return {
                    "status": "error",
                    "message": (
                        "No response from NameNode."
                    ),
                }

            if response.get("status") != "success":

                return response

            # =================================================
            # 2. Send chunks to selected DataNodes
            # =================================================

            failed_chunks = []
            confirmed_replicas = {}
            confirmed_checksums = {}

            for chunk_idx, chunk_info in enumerate(
                response["chunks"]
            ):

                chunk_id = chunk_info[
                    "chunk_id"
                ]

                chunk_data = chunks[
                    chunk_idx
                ]["data"]

                datanode_ports = chunk_info.get(
                    "datanodes",
                    [],
                )

                successful_replicas = 0

                for datanode_port in datanode_ports:

                    datanode_ip = (
                        DATANODE_PORT_TO_IP.get(
                            datanode_port
                        )
                    )

                    if datanode_ip is None:

                        print(
                            f"Unknown DataNode port: "
                            f"{datanode_port}"
                        )

                        continue

                    try:

                        datanode_sock = socket.socket(
                            socket.AF_INET,
                            socket.SOCK_STREAM,
                        )

                        datanode_sock.connect(
                            (
                                datanode_ip,
                                datanode_port,
                            )
                        )

                        # ------------------------------------
                        # Send store request.
                        # ------------------------------------

                        send_message(
                            datanode_sock,
                            {
                                "type": "store_chunk",
                                "chunk_id": chunk_id,
                                "size": len(chunk_data),
                            },
                        )

                        # ------------------------------------
                        # Send binary chunk data.
                        # ------------------------------------

                        datanode_sock.sendall(
                            chunk_data
                        )

                        # ------------------------------------
                        # Wait for DataNode ACK.
                        # ------------------------------------

                        ack = receive_message(
                            datanode_sock
                        )

                        datanode_sock.close()

                        if ack and ack.get(
                            "status"
                        ) == "success":

                            successful_replicas += 1
                            confirmed_replicas.setdefault(
                                chunk_id,
                                [],
                            ).append(datanode_port)
                            confirmed_checksums[chunk_id] = ack.get(
                                "checksum"
                            )
                            print(
                                f"Stored {chunk_id} "
                                f"on datanode "
                                f"{datanode_ip}:"
                                f"{datanode_port} "
                                f"(replica {successful_replicas}/{len(datanode_ports)})"
                            )

                        else:

                            print(
                                f"DataNode did not "
                                f"confirm storage of "
                                f"{chunk_id} on "
                                f"{datanode_ip}:"
                                f"{datanode_port}"
                            )

                    except Exception as exc:

                        print(
                            f"Error storing chunk "
                            f"{chunk_id} on "
                            f"datanode "
                            f"{datanode_ip}:"
                            f"{datanode_port}: "
                            f"{exc}"
                        )

                # Verify replication factor was met
                if successful_replicas < len(datanode_ports):
                    print(
                        f"WARNING: {chunk_id} only has {successful_replicas}/{len(datanode_ports)} replicas. "
                        f"Re-replication will handle this."
                    )
                    failed_chunks.append(chunk_id)

            # =================================================
            # Commit only the replicas acknowledged by DataNodes.
            # =================================================

            confirmed_chunks = [
                {
                    "chunk_id": chunk_id,
                    "datanodes": datanodes,
                    "checksum": confirmed_checksums.get(chunk_id),
                }
                for chunk_id, datanodes in confirmed_replicas.items()
            ]

            finalize_sock = socket.socket(
                socket.AF_INET,
                socket.SOCK_STREAM,
            )
            try:
                finalize_sock.connect(
                    (self.namenode_host, self.namenode_port)
                )
                send_message(
                    finalize_sock,
                    {
                        "type": "finalize_upload",
                        "upload_id": response.get("upload_id"),
                        "chunks": confirmed_chunks,
                    },
                )
                finalize_response = receive_message(finalize_sock)
            except (ConnectionError, OSError) as exc:
                finalize_response = {
                    "status": "error",
                    "message": f"Could not finalize upload: {exc}",
                }
            finally:
                finalize_sock.close()

            if not finalize_response:
                return {
                    "status": "error",
                    "message": "No response while finalizing upload.",
                }

            # =================================================
            # Upload completed
            # =================================================

            if finalize_response.get("status") == "error":
                for item in confirmed_chunks:
                    for datanode_port in item.get("datanodes", []):
                        datanode_ip = DATANODE_PORT_TO_IP.get(
                            datanode_port
                        )
                        if datanode_ip is None:
                            continue
                        try:
                            with socket.socket(
                                socket.AF_INET,
                                socket.SOCK_STREAM,
                            ) as cleanup_sock:
                                cleanup_sock.connect(
                                    (datanode_ip, datanode_port)
                                )
                                send_message(
                                    cleanup_sock,
                                    {
                                        "type": "delete_chunk",
                                        "chunk_id": item["chunk_id"],
                                    },
                                )
                                receive_message(cleanup_sock)
                        except (ConnectionError, OSError):
                            pass
                return finalize_response

            if failed_chunks:
                return {
                    "status": finalize_response.get(
                        "status", "partial_success"
                    ),
                    "message": (
                        f"Uploaded {len(chunks)} chunks, "
                        f"but {len(failed_chunks)} chunks have insufficient replicas. "
                        f"Re-replication will restore them."
                    ),
                    "filename": filename,
                    "file_size": total_file_size,
                    "chunk_count": len(chunks),
                    "failed_chunks": failed_chunks,
                }

            return {
                "status": "success",
                "message": (
                    f"Uploaded {len(chunks)} chunks successfully"
                ),
                "filename": filename,
                "file_size": total_file_size,
                "chunk_count": len(chunks),
            }

        except Exception as exc:

            print(
                f"Upload error: {exc}"
            )

            return {
                "status": "error",
                "message": str(exc),
            }

    # =====================================================
    # DELETE FILE
    # =====================================================

    def delete_file(self, filename):
        """
        Delete a file from HDFS.
        """

        try:

            namenode_sock = socket.socket(
                socket.AF_INET,
                socket.SOCK_STREAM,
            )

            namenode_sock.connect(
                (
                    self.namenode_host,
                    self.namenode_port,
                )
            )

            send_message(
                namenode_sock,
                {
                    "type": "delete_file",
                    "filename": filename,
                },
            )

            response = receive_message(
                namenode_sock
            )

            namenode_sock.close()

            if not response:

                return {
                    "status": "error",
                    "message": (
                        "No response from NameNode."
                    ),
                }

            return response

        except Exception as exc:

            print(
                f"Delete error: {exc}"
            )

            return {
                "status": "error",
                "message": str(exc),
            }

    # =====================================================
    # RENAME FILE
    # =====================================================

    def rename_file(self, filename, new_filename):
        """
        Rename a file in HDFS.
        """

        try:

            namenode_sock = socket.socket(
                socket.AF_INET,
                socket.SOCK_STREAM,
            )

            namenode_sock.connect(
                (
                    self.namenode_host,
                    self.namenode_port,
                )
            )

            send_message(
                namenode_sock,
                {
                    "type": "rename_file",
                    "filename": filename,
                    "new_filename": new_filename,
                },
            )

            response = receive_message(
                namenode_sock
            )

            namenode_sock.close()

            if not response:

                return {
                    "status": "error",
                    "message": (
                        "No response from NameNode."
                    ),
                }

            return response

        except Exception as exc:

            print(
                f"Rename error: {exc}"
            )

            return {
                "status": "error",
                "message": str(exc),
            }

    # =====================================================
    # CREATE DIRECTORY
    # =====================================================

    def create_directory(self, dirname):
        """
        Create a directory in HDFS.
        """

        try:

            namenode_sock = socket.socket(
                socket.AF_INET,
                socket.SOCK_STREAM,
            )

            namenode_sock.connect(
                (
                    self.namenode_host,
                    self.namenode_port,
                )
            )

            send_message(
                namenode_sock,
                {
                    "type": "mkdir",
                    "dirname": dirname,
                },
            )

            response = receive_message(
                namenode_sock
            )

            namenode_sock.close()

            if not response:

                return {
                    "status": "error",
                    "message": (
                        "No response from NameNode."
                    ),
                }

            return response

        except Exception as exc:

            print(
                f"Create directory error: {exc}"
            )

            return {
                "status": "error",
                "message": str(exc),
            }

    # =====================================================
    # LIST DIRECTORY
    # =====================================================

    def list_directory(self, dirname=""):
        """
        List contents of a directory in HDFS.
        """

        try:

            namenode_sock = socket.socket(
                socket.AF_INET,
                socket.SOCK_STREAM,
            )

            namenode_sock.connect(
                (
                    self.namenode_host,
                    self.namenode_port,
                )
            )

            send_message(
                namenode_sock,
                {
                    "type": "list_directory",
                    "dirname": dirname,
                },
            )

            response = receive_message(
                namenode_sock
            )

            namenode_sock.close()

            if not response:

                return {
                    "status": "error",
                    "message": (
                        "No response from NameNode."
                    ),
                }

            return response

        except Exception as exc:

            print(
                f"List directory error: {exc}"
            )

            return {
                "status": "error",
                "message": str(exc),
            }

    # =====================================================
    # DELETE DIRECTORY
    # =====================================================

    def delete_directory(self, dirname):
        """
        Delete a directory from HDFS (must be empty).
        """

        try:

            namenode_sock = socket.socket(
                socket.AF_INET,
                socket.SOCK_STREAM,
            )

            namenode_sock.connect(
                (
                    self.namenode_host,
                    self.namenode_port,
                )
            )

            send_message(
                namenode_sock,
                {
                    "type": "rmdir",
                    "dirname": dirname,
                },
            )

            response = receive_message(
                namenode_sock
            )

            namenode_sock.close()

            if not response:

                return {
                    "status": "error",
                    "message": (
                        "No response from NameNode."
                    ),
                }

            return response

        except Exception as exc:

            print(
                f"Delete directory error: {exc}"
            )

            return {
                "status": "error",
                "message": str(exc),
            }

    # =====================================================
    # DOWNLOAD
    # =====================================================

    def download_file(
        self,
        filename,
        output_path,
    ):
        """
        Download a file from HDFS.
        """

        try:

            # =================================================
            # 1. Ask NameNode for chunk locations
            # =================================================

            namenode_sock = socket.socket(
                socket.AF_INET,
                socket.SOCK_STREAM,
            )

            namenode_sock.connect(
                (
                    self.namenode_host,
                    self.namenode_port,
                )
            )

            send_message(
                namenode_sock,
                {
                    "type": "download_file",
                    "filename": filename,
                },
            )

            response = receive_message(
                namenode_sock
            )

            namenode_sock.close()

            if not response:

                return {
                    "status": "error",
                    "message": (
                        "No response from NameNode."
                    ),
                }

            if response.get("status") != "success":

                return response

            # =================================================
            # 2. Fetch all chunks
            # =================================================

            all_chunks = []

            for chunk_info in response["chunks"]:

                chunk_id = chunk_info[
                    "chunk_id"
                ]

                datanodes = chunk_info.get(
                    "datanodes",
                    [],
                )

                chunk_data = None

                for datanode in datanodes:

                    if isinstance(datanode, (list, tuple)):
                        datanode_ip, datanode_port = datanode
                    else:
                        datanode_port = datanode
                        datanode_ip = DATANODE_PORT_TO_IP.get(
                            datanode_port
                        )

                    if datanode_ip is None:
                        continue

                    try:

                        datanode_sock = socket.socket(
                            socket.AF_INET,
                            socket.SOCK_STREAM,
                        )
                        datanode_sock.settimeout(3)  # 3 second timeout

                        datanode_sock.connect(
                            (
                                datanode_ip,
                                datanode_port,
                            )
                        )

                        send_message(
                            datanode_sock,
                            {
                                "type": "get_chunk",
                                "chunk_id": chunk_id,
                            },
                        )

                        # ------------------------------------
                        # Phase 15: Receive JSON response with size and checksum.
                        # ------------------------------------

                        response = receive_chunk_response(datanode_sock)

                        if not response or response.get("status") != "success":
                            datanode_sock.close()
                            continue

                        size = response.get("file_size", 0)
                        checksum = response.get("checksum", "")

                        if size < 0:
                            datanode_sock.close()
                            continue

                        # ------------------------------------
                        # Receive complete chunk.
                        # ------------------------------------

                        if response.get("legacy_payload") is not None:
                            chunk_data = response["legacy_payload"]
                        else:
                            chunk_data = receive_file(
                                datanode_sock,
                                size,
                            )

                        # ------------------------------------
                        # Phase 15: Verify SHA-256 checksum.
                        # ------------------------------------

                        if (
                            chunk_data
                            and checksum
                            and not verify_checksum(
                                chunk_data,
                                checksum,
                            )
                        ):

                            print(
                                f"Checksum mismatch for "
                                f"chunk {chunk_id}"
                            )

                            datanode_sock.close()
                            continue

                        datanode_sock.close()

                        if chunk_data is not None:

                            break

                    except Exception as exc:

                        print(
                            f"Error fetching chunk "
                            f"{chunk_id} from "
                            f"{datanode_ip}:"
                            f"{datanode_port}: "
                            f"{exc}"
                        )

                        continue

                if chunk_data is None:

                    return {
                        "status": "error",
                        "message": (
                            f"Could not retrieve "
                            f"chunk {chunk_id} from replicas {datanodes}"
                        ),
                    }

                all_chunks.append(
                    chunk_data
                )

            # =================================================
            # 3. Combine chunks
            # =================================================

            file_data = b"".join(
                all_chunks
            )

            # =================================================
            # 4. Write downloaded file
            # =================================================

            with open(
                output_path,
                "wb",
            ) as output_file:

                output_file.write(
                    file_data
                )

            return {
                "status": "success",
                "message": (
                    f"Downloaded {filename}"
                ),
                "file_size": len(file_data),
            }

        except Exception as exc:

            print(
                f"Download error: {exc}"
            )

            return {
                "status": "error",
                "message": str(exc),
            }


# =========================================================
# GLOBAL CLIENT
# =========================================================

hdfs_client = HDFSClient()


# =========================================================
# DASHBOARD ROUTE
# =========================================================

@app.route("/")
def dashboard():

    return render_template(
        "dashboard.html"
    )


# =========================================================
# UPLOAD API
# =========================================================

@app.route(
    "/api/upload",
    methods=["POST"],
)
def upload_file_route():

    if "file" not in request.files:

        return jsonify(
            {
                "status": "error",
                "message": "No file provided",
            }
        ), 400

    file = request.files["file"]

    if not file.filename:

        return jsonify(
            {
                "status": "error",
                "message": "No filename provided",
            }
        ), 400

    temp_path = os.path.join(
        app.config["UPLOAD_FOLDER"],
        file.filename,
    )

    try:

        file.save(
            temp_path
        )

        result = hdfs_client.upload_file(
            temp_path
        )

        return jsonify(
            result
        )

    except Exception as exc:

        return jsonify(
            {
                "status": "error",
                "message": str(exc),
            }
        ), 500

    finally:

        if os.path.exists(temp_path):

            try:
                os.remove(
                    temp_path
                )

            except OSError:
                pass


# =========================================================
# DOWNLOAD API
# =========================================================

@app.route(
    "/api/download/<filename>",
    methods=["GET"],
)
def download_file_route(filename):

    # Download to temp file
    output_path = os.path.join(
        app.config["UPLOAD_FOLDER"],
        f"downloaded_{filename}",
    )

    result = hdfs_client.download_file(
        filename,
        output_path,
    )

    if result.get("status") != "success":

        return jsonify(
            result
        ), 400

    # Send file to browser as download
    return send_file(
        output_path,
        as_attachment=True,
        download_name=filename,
    )


# =========================================================
# FILE LIST API
# =========================================================

@app.route(
    "/api/files",
    methods=["GET"],
)
def list_files_route():

    try:

        namenode_sock = socket.socket(
            socket.AF_INET,
            socket.SOCK_STREAM,
        )

        namenode_sock.connect(
            (
                NAMENODE_IP,
                NAMENODE_PORT,
            )
        )

        send_message(
            namenode_sock,
            {
                "type": "list_files",
            },
        )

        response = receive_message(
            namenode_sock
        )

        namenode_sock.close()

        return jsonify(
            response
        )

    except Exception as exc:

        return jsonify(
            {
                "status": "error",
                "message": str(exc),
            }
        ), 500


# =========================================================
# FILE INFORMATION API
# =========================================================

@app.route(
    "/api/files/<path:filename>",
    methods=["GET"],
)
def file_info_route(filename):

    try:

        namenode_sock = socket.socket(
            socket.AF_INET,
            socket.SOCK_STREAM,
        )

        namenode_sock.connect(
            (
                NAMENODE_IP,
                NAMENODE_PORT,
            )
        )

        send_message(
            namenode_sock,
            {
                "type": "get_file_info",
                "filename": filename,
            },
        )

        response = receive_message(
            namenode_sock
        )

        namenode_sock.close()

        return jsonify(
            response
        )

    except Exception as exc:

        return jsonify(
            {
                "status": "error",
                "message": str(exc),
            }
        ), 500


# =========================================================
# DELETE FILE API
# =========================================================

@app.route(
    "/api/files/<path:filename>",
    methods=["DELETE"],
)
def delete_file_route(filename):

    result = hdfs_client.delete_file(
        filename
    )

    if result.get("status") not in ("success", "partial_success"):

        return jsonify(
            result
        ), 400

    return jsonify(
        result
    )


# =========================================================
# RENAME FILE API
# =========================================================

@app.route(
    "/api/files/<path:filename>/rename",
    methods=["POST"],
)
def rename_file_route(filename):

    data = request.get_json()

    if not data or "new_filename" not in data:

        return jsonify(
            {
                "status": "error",
                "message": "new_filename is required",
            }
        ), 400

    new_filename = data["new_filename"]

    result = hdfs_client.rename_file(
        filename,
        new_filename,
    )

    if result.get("status") != "success":

        return jsonify(
            result
        ), 400

    return jsonify(
        result
    )


# =========================================================
# CREATE DIRECTORY API
# =========================================================

@app.route(
    "/api/directories",
    methods=["POST"],
)
def create_directory_route():

    data = request.get_json()

    if not data or "dirname" not in data:

        return jsonify(
            {
                "status": "error",
                "message": "dirname is required",
            }
        ), 400

    dirname = data["dirname"]

    result = hdfs_client.create_directory(
        dirname
    )

    if result.get("status") != "success":

        return jsonify(
            result
        ), 400

    return jsonify(
        result
    )


# =========================================================
# LIST DIRECTORY API
# =========================================================

@app.route(
    "/api/directories/",
    methods=["GET"],
)
@app.route(
    "/api/directories",
    methods=["GET"],
)
@app.route(
    "/api/directories/<path:dirname>",
    methods=["GET"],
)
def list_directory_route(dirname=""):

    result = hdfs_client.list_directory(
        dirname
    )

    if result.get("status") != "success":

        return jsonify(
            result
        ), 404

    # Enhance file information with real metadata
    contents = result.get("contents", [])
    enhanced_contents = []

    for item in contents:
        if item.get("type") == "file":
            # Get detailed file info
            try:
                namenode_sock = socket.socket(
                    socket.AF_INET,
                    socket.SOCK_STREAM,
                )

                namenode_sock.connect(
                    (
                        NAMENODE_IP,
                        NAMENODE_PORT,
                    )
                )

                send_message(
                    namenode_sock,
                    {
                        "type": "get_file_info",
                        "filename": item.get("name"),
                    },
                )

                info_response = receive_message(
                    namenode_sock
                )

                namenode_sock.close()

                if info_response and info_response.get("status") == "success":
                    chunks = info_response.get("chunks", [])
                    replica_counts = {}
                    for chunk in chunks:
                        for node in chunk.get("datanodes", []):
                            replica_counts[node] = replica_counts.get(node, 0) + 1
                    item["size"] = info_response.get("file_size", 0)
                    item["chunks"] = info_response.get(
                        "chunk_count",
                        len(chunks),
                    )
                    item["replication"] = (
                        f"{min((len(chunk.get('datanodes', [])) for chunk in chunks), default=0)}"
                        f"/{REPLICATION_FACTOR}"
                    )
                    item["replica_distribution"] = ", ".join(
                        f"{node}: {count} chunks"
                        for node, count in sorted(replica_counts.items())
                    )
                    item["replica_distribution"] += " | " + "; ".join(
                        f"C{chunk.get('chunk_index', index)}: "
                        f"{', '.join(chunk.get('datanodes', []))}"
                        for index, chunk in enumerate(chunks)
                    )
                    item["chunk_placements"] = [
                        {
                            "index": chunk.get("chunk_index", index),
                            "nodes": chunk.get("datanodes", []),
                        }
                        for index, chunk in enumerate(chunks)
                    ]
                    item["replication_status"] = (
                        "HEALTHY"
                        if all(
                            len(chunk.get("datanodes", [])) >= REPLICATION_FACTOR
                            for chunk in chunks
                        )
                        else "UNDER-REPLICATED"
                    )
                else:
                    item["size"] = 0
                    item["chunks"] = 0
                    item["replication"] = "Unknown"
                    item["replica_distribution"] = "Unknown"
            except Exception:
                item["size"] = 0
                item["chunks"] = 0
                item["replication"] = "Unknown"

        enhanced_contents.append(item)

    result["contents"] = enhanced_contents

    return jsonify(
        result
    )


# =========================================================
# DELETE DIRECTORY API
# =========================================================

@app.route(
    "/api/directories/<path:dirname>",
    methods=["DELETE"],
)
def delete_directory_route(dirname):

    result = hdfs_client.delete_directory(
        dirname
    )

    if result.get("status") not in ("success", "partial_success"):

        return jsonify(
            result
        ), 400

    return jsonify(
        result
    )


# =========================================================
# STATUS API
# =========================================================

@app.route("/api/events", methods=["GET"])
def events_route():
    with event_history_lock:
        events = list(event_history)
    return jsonify({"status": "success", "events": events})

@app.route(
    "/api/status",
    methods=["GET"],
)
def status_route():

    try:

        namenode_sock = socket.socket(
            socket.AF_INET,
            socket.SOCK_STREAM,
        )

        namenode_sock.connect(
            (
                NAMENODE_IP,
                NAMENODE_PORT,
            )
        )

        send_message(
            namenode_sock,
            {
                "type": "cluster_status",
            },
        )

        response = receive_message(
            namenode_sock
        )

        namenode_sock.close()

        if response and response.get(
            "status"
        ) == "success":

            # Transform cluster_status response to match UI expected format
            cluster = response.get("cluster", {})
            datanodes = response.get("datanodes", [])

            # Determine system health
            alive = cluster.get("alive_datanodes", 0)
            total = cluster.get("total_datanodes", 0)
            under_replicated = cluster.get("under_replicated_chunks", 0)

            if alive == 0:
                health = "critical"
            elif alive < total or under_replicated > 0:
                health = "degraded"
            else:
                health = "healthy"

            # Calculate total storage from DataNode capacities
            total_storage = 0
            for dn in datanodes:
                capacity = dn.get("capacity", {})
                total_storage += capacity.get("used_bytes", 0)

            # Map datanode format to match UI expectations
            mapped_datanodes = []
            for dn in datanodes:
                mapped_datanodes.append({
                    "id": dn.get("datanode_id", "Unknown"),
                    "address": dn.get("address", "N/A"),
                    "status": dn.get("status", "UNKNOWN"),
                    "last_check": dn.get("last_heartbeat", 0),
                    "chunk_count": dn.get("chunk_count", 0),
                    "capacity": dn.get("capacity", {}),
                })

            return jsonify({
                "status": "success",
                "statistics": {
                    "total_files": cluster.get("total_files", 0),
                    "total_chunks": cluster.get("total_chunks", 0),
                    "total_storage_bytes": total_storage,
                    "alive_datanodes": alive,
                    "total_datanodes": total,
                },
                "system": {
                    "health": health,
                },
                "datanodes": mapped_datanodes,
            })

        return jsonify(
            {
                "status": "offline",
                "namenode": "unavailable",
            }
        )

    except Exception as exc:

        return jsonify(
            {
                "status": "offline",
                "namenode": "unavailable",
                "message": str(exc),
            }
        )


# =========================================================
# START CLIENT
# =========================================================

def start_client():

    print(
        f"Starting HDFS Client on "
        f"{CLIENT_IP}:"
        f"{CLIENT_PORT}"
    )

    threading.Thread(
        target=start_kafka_event_consumer,
        daemon=True,
        name="KafkaEventConsumer",
    ).start()

    app.run(
        host=CLIENT_IP,
        port=CLIENT_PORT,
        debug=False,
        use_reloader=False,
    )


# =========================================================
# MAIN
# =========================================================

if __name__ == "__main__":
    start_client()