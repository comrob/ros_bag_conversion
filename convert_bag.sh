#!/bin/bash

# Configuration
SCRIPT_DIR="$(dirname "$(realpath "$0")")"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yml"
SRC_DIR="$SCRIPT_DIR/src"

# Check Usage
if [ -z "$1" ]; then
    echo "Usage: convert_bag <file_or_folder> [options]"
    exit 1
fi

# 1. Determine Mount Point (The 'Stage')
FIRST_ARG_ABS=$(realpath "$1")
if [ -d "$FIRST_ARG_ABS" ]; then
    MOUNT_HOST="$FIRST_ARG_ABS"
else
    MOUNT_HOST=$(dirname "$FIRST_ARG_ABS")
fi

# 2. Construct Arguments
PY_ARGS=""
for var in "$@"; do
    # Skip flags (starts with -)
    if [[ "$var" == -* ]]; then
        PY_ARGS="$PY_ARGS $var"
        continue
    fi

    # Check mapping for anything that isn't a flag
    # realpath -m resolves paths even if they don't exist (e.g. output dirs)
    ABS_VAR=$(realpath -m "$var")
    
    # If the path (existing or theoretical) lies within our mount point...
    if [[ "$ABS_VAR" == "$MOUNT_HOST"* ]]; then
            # ...pass it as a relative path to Python
            REL_PATH=$(realpath -m --relative-to="$MOUNT_HOST" "$ABS_VAR")
            PY_ARGS="$PY_ARGS $REL_PATH"
    else
            # ...otherwise pass it raw (e.g. "200M", "humble")
            PY_ARGS="$PY_ARGS $var"
    fi
done

# 3. Run
echo "[HOST] Mounting Data:   $MOUNT_HOST"
echo "[HOST] Working Dir:     /data"

export HOST_DATA_DIR="$MOUNT_HOST"
export CURRENT_UID=$(id -u)
export CURRENT_GID=$(id -g)

docker compose -f "$COMPOSE_FILE" run --rm \
    -w /data \
    converter \
    python3 /home/dev/src/convert.py $PY_ARGS