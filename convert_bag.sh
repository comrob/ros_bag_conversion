#!/bin/bash

# Usage Check
if [ -z "$1" ]; then
    echo "Usage: convert_bag <input_path> [options]"
    exit 1
fi

SCRIPT_DIR="$(dirname "$(realpath "$0")")"

# --- 1. Safety Check: Verify all files are in the same folder ---
# We take the directory of the first argument as the reference.
REF_DIR=$(dirname "$(realpath "$1")")

for arg in "$@"; do
    # Skip flags
    if [[ "$arg" == -* ]]; then continue; fi
    
    # Get absolute path of current arg
    CUR_PATH=$(realpath "$arg")
    
    # If it's a directory, we check its parent (because the dir itself is the input)
    if [ -d "$CUR_PATH" ]; then
        CUR_DIR=$(dirname "$CUR_PATH")
    else
        CUR_DIR=$(dirname "$CUR_PATH")
    fi

    # Compare against reference
    if [ "$CUR_DIR" != "$REF_DIR" ]; then
        echo "[HOST][ERROR] All input files must be in the same directory."
        echo "[HOST]  Reference: $REF_DIR"
        echo "[HOST]  Mismatch:  $CUR_DIR ($arg)"
        exit 1
    fi
done

# --- 2. Prepare Docker Mounts ---
# We mount the directory containing the inputs to /data
HOST_MOUNT_DIR="$REF_DIR"
echo "[HOST] [INFO] Mounting Host: $HOST_MOUNT_DIR -> Container: /data"

# --- 3. Build Python Arguments ---
# We convert host paths to container paths (relative to /data)
PY_ARGS=""

for arg in "$@"; do
    if [[ "$arg" == -* ]]; then
        # Pass flags through
        PY_ARGS="$PY_ARGS $arg"
    else
        # Just take the filename (or folder name)
        NAME=$(basename "$arg")
        PY_ARGS="$PY_ARGS /data/$NAME"
    fi
done

# --- 4. Run Docker ---
HOST_DATA_DIR="$HOST_MOUNT_DIR" \
CURRENT_UID=$(id -u) \
CURRENT_GID=$(id -g) \
docker compose -f "$SCRIPT_DIR/docker-compose.yml" run --rm converter \
    python3 /home/dev/convert.py $PY_ARGS