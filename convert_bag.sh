#!/bin/bash

# Find where this script and docker-compose.yml are located
SCRIPT_DIR="$(dirname "$(realpath "$0")")"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yml"

# Usage Check
if [ -z "$1" ]; then
    echo "Usage: convert_bag <file_or_folder> [options]"
    exit 1
fi

# 1. Determine Mount Point (HOST_DATA_DIR)
# We find the absolute path of the input to determine the parent folder.
FIRST_ARG_ABS=$(realpath "$1")

if [ -d "$FIRST_ARG_ABS" ]; then
    # Input is a folder
    MOUNT_HOST=$(dirname "$FIRST_ARG_ABS")
else
    # Input is a file
    MOUNT_HOST=$(dirname "$FIRST_ARG_ABS")
fi

# 2. Construct Python Arguments
# We rewrite the host arguments to be relative to the container's /data
PY_ARGS=""
for var in "$@"
do
    if [[ "$var" == -* ]]; then
        # Pass flags (e.g. --series) directly
        PY_ARGS="$PY_ARGS $var"
    else
        ABS_VAR=$(realpath "$var")
        # Check if the file is actually inside our calculated mount
        if [[ "$ABS_VAR" == "$MOUNT_HOST"* ]]; then
             # Remove the host prefix and prepend /data
             # e.g. /home/user/data/bag.bag -> /data/bag.bag
             REL_PATH="${ABS_VAR#$MOUNT_HOST}"
             PY_ARGS="$PY_ARGS /data${REL_PATH}"
        else
             echo "[WARN] Argument $var is outside the mounted directory ($MOUNT_HOST). It will be invisible to the container."
        fi
    fi
done

# 3. Run Docker Compose
# We pass the calculated directory and user ID as environment variables.
# docker compose will substitute ${HOST_DATA_DIR}, ${CURRENT_UID}, etc.

echo "[HOST] Mounting: $MOUNT_HOST"

export HOST_DATA_DIR="$MOUNT_HOST"
export CURRENT_UID=$(id -u)
export CURRENT_GID=$(id -g)

# --rm: Clean up container after run
# converter: The service name in docker-compose.yml
# python3 ...: The command to override the default
docker compose -f "$COMPOSE_FILE" run --rm converter \
    python3 /home/dev/convert.py $PY_ARGS