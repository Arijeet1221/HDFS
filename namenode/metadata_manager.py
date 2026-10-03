# MINI_HDFS/namenode/metadata_manager.py

"""
Persistent metadata manager for the HDFS NameNode.

Phase 1.1
----------
Provides:

- Loading metadata from disk
- Saving metadata to disk
- Atomic metadata updates
- Thread-safe metadata access
- Safe handling when metadata.json does not exist
"""

import json
import os
import tempfile
import threading


# ============================================================
# METADATA FILE
# ============================================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

METADATA_FILE = os.path.join(
    BASE_DIR,
    "metadata.json",
)


# ============================================================
# THREAD SAFETY
# ============================================================

metadata_lock = threading.RLock()


# ============================================================
# LOAD METADATA
# ============================================================

def load_metadata():
    """
    Load NameNode metadata from metadata.json.

    If the metadata file does not exist, return an empty
    dictionary.

    If the metadata file is invalid, raise an error instead
    of silently destroying existing metadata.
    """

    with metadata_lock:

        if not os.path.exists(METADATA_FILE):

            print(
                "[Metadata] metadata.json not found. "
                "Starting with empty metadata."
            )

            return {}

        try:

            with open(
                METADATA_FILE,
                "r",
                encoding="utf-8",
            ) as file:

                data = json.load(file)

            if not isinstance(data, dict):

                raise ValueError(
                    "metadata.json root must be a JSON object."
                )

            print(
                f"[Metadata] Loaded metadata for "
                f"{len(data)} file(s)."
            )

            return data

        except json.JSONDecodeError as exc:

            print(
                "[Metadata] ERROR: metadata.json is corrupted."
            )

            raise RuntimeError(
                "NameNode metadata file is corrupted."
            ) from exc

        except OSError as exc:

            print(
                f"[Metadata] ERROR reading metadata: {exc}"
            )

            raise


# ============================================================
# SAVE METADATA
# ============================================================

def save_metadata(metadata):
    """
    Persist NameNode metadata to disk.

    Uses an atomic replacement strategy:

        metadata.json.tmp
                 ↓
        write complete file
                 ↓
        replace metadata.json

    This prevents a partially-written metadata file if the
    NameNode crashes during a write.
    """

    if not isinstance(metadata, dict):

        raise TypeError(
            "metadata must be a dictionary."
        )

    with metadata_lock:

        directory = os.path.dirname(
            METADATA_FILE
        )

        os.makedirs(
            directory,
            exist_ok=True,
        )

        temp_path = None

        try:

            # ------------------------------------------------
            # Create temporary file in the same directory.
            # This is important because os.replace() is
            # atomic when source and destination are on the
            # same filesystem.
            # ------------------------------------------------

            fd, temp_path = tempfile.mkstemp(
                prefix="metadata_",
                suffix=".tmp",
                dir=directory,
            )

            with os.fdopen(
                fd,
                "w",
                encoding="utf-8",
            ) as file:

                json.dump(
                    metadata,
                    file,
                    indent=4,
                    ensure_ascii=False,
                )

                file.flush()

                os.fsync(
                    file.fileno()
                )

            # ------------------------------------------------
            # Atomically replace the old metadata file.
            # ------------------------------------------------

            os.replace(
                temp_path,
                METADATA_FILE,
            )

            temp_path = None

            print(
                f"[Metadata] Saved metadata for "
                f"{len(metadata)} file(s)."
            )

        except Exception as exc:

            print(
                f"[Metadata] ERROR saving metadata: {exc}"
            )

            raise

        finally:

            # ------------------------------------------------
            # Clean up temporary file if something failed.
            # ------------------------------------------------

            if (
                temp_path is not None
                and os.path.exists(temp_path)
            ):

                try:
                    os.remove(temp_path)

                except OSError:
                    pass


# ============================================================
# METADATA FILE PATH
# ============================================================

def get_metadata_file():
    """
    Return the absolute path of metadata.json.
    """

    return METADATA_FILE