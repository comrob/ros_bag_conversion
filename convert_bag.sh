#!/bin/bash

# Configuration
SCRIPT_DIR="$(dirname "$(realpath "$0")")"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yml"

# Check Usage
if [ -z "$1" ]; then
    echo "Usage: convert_bag <file_or_folder> [options]"
    exit 1
fi

# ==============================================================================
# 1. INTELLIGENT MOUNT POINT DETECTION (Common Ancestor)
# ==============================================================================

# Initialize Mount Point with the directory of the first argument
# We use 'realpath -m' to handle paths that might not exist yet (like output dirs)
MOUNT_HOST=$(dirname "$(realpath -m "$1")")

# Iterate over ALL arguments to find the common root
for var in "$@"; do
    # Skip flags
    if [[ "$var" == -* ]]; then continue; fi

    # Get absolute path of current arg
    ABS_PATH=$(realpath -m "$var")
    
    # [FIX START] --------------------------------------------------------------
    # Only allow ACTUAL FILES/DIRS to influence the mount point.
    # This prevents strings (like topic names '/foo/bar') from triggering a 
    # traverse up to the system root '/', which breaks path relativization.
    if [ ! -e "$ABS_PATH" ]; then
        continue
    fi
    # [FIX END] ----------------------------------------------------------------
    
    # While the current arg is NOT inside the calculated MOUNT_HOST...
    # We move the MOUNT_HOST one level up (parent directory).
    while [[ "$ABS_PATH" != "$MOUNT_HOST"* ]]; do
        MOUNT_HOST=$(dirname "$MOUNT_HOST")
        
        # Safety break: stop if we hit root to prevent infinite loops
        if [[ "$MOUNT_HOST" == "/" ]]; then break; fi
    done
done

# ==============================================================================
# 2. PATH RELATIVIZATION
# ==============================================================================
PY_ARGS=""

for var in "$@"; do
    # Pass flags through unchanged
    if [[ "$var" == -* ]]; then
        PY_ARGS="$PY_ARGS $var"
        continue
    fi

    ABS_VAR=$(realpath -m "$var")
    
    # If the path lies within our calculated mount point...
    if [[ "$ABS_VAR" == "$MOUNT_HOST"* ]]; then
            # ...pass it as a relative path to Python
            REL_PATH=$(realpath -m --relative-to="$MOUNT_HOST" "$ABS_VAR")
            PY_ARGS="$PY_ARGS $REL_PATH"
    else
            # Fallback: Pass the string literally (This now preserves topics!)
            PY_ARGS="$PY_ARGS $var"
    fi
done

# ==============================================================================
# 3. EXECUTION
# ==============================================================================
# echo "[DEBUG] Mount Point: $MOUNT_HOST"

export HOST_DATA_DIR="$MOUNT_HOST"
export CURRENT_UID=$(id -u)
export CURRENT_GID=$(id -g)

# We mount the detected Common Ancestor to /data
docker compose -f "$COMPOSE_FILE" run --rm \
    -e HOST_DATA_DIR \
    -w /data \
    converter \
    python3 /home/dev/src/convert.py $PY_ARGS