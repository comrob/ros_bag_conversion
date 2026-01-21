#!/bin/bash
set -e  # Exit immediately on error

# ==============================================================================
# STEP 0: LOCATION INDEPENDENCE
# ==============================================================================
# 1. Get the absolute path of the directory containing this script
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

# 2. Move into that directory so all relative paths work
cd "$SCRIPT_DIR"

echo "[INFO] Running demo from: $SCRIPT_DIR"

# --- Configuration (Relative to SCRIPT_DIR) ---
DATA_DIR="data"
LAYOUT_FILE="foxglove_layout.json"
CONVERTER_SCRIPT="../convert_bag.sh"  # Now safely points to parent of SCRIPT_DIR

# Google Drive IDs
ID_BAG_0="1u-dvvvne-OXfz2YMDGVg2FBtSolf2pNk"
ID_BAG_1="1TQjpWbwi5CyzFpfE1lIxQR-w3Q16340K"
ID_BAG_2="1xlR7QFL9wKGkANAENt3HxEPX7KY2IbMH"
ID_LAYOUT="15DyLCDZd7rhZ3nwSuKCv-ZdzOwjEyt6H"

# --- Helper Functions ---
function download_if_missing() {
    local file_id=$1
    local output_name=$2
    
    if [ ! -f "$output_name" ]; then
        echo "[DOWNLOAD] Fetching $output_name..."
        gdown "$file_id" -O "$output_name"
    else
        echo "[SKIP] $output_name already exists."
    fi
}

function print_header() {
    echo -e "\n========================================"
    echo "  $1"
    echo "========================================"
}

# ==============================================================================
# STEP 1: VIRTUAL ENVIRONMENT SETUP
# ==============================================================================
print_header "STEP 1: Checking Dependencies"

# Ensure the setup script exists and is executable
if [ -f "./setup_venv.sh" ]; then
    if [ ! -x "./setup_venv.sh" ]; then chmod +x "./setup_venv.sh"; fi
    ./setup_venv.sh
else
    echo "[ERR] setup_venv.sh not found in $SCRIPT_DIR!"
    exit 1
fi

# Activate the virtual environment
source .venv/bin/activate
echo "[INFO] Virtual environment activated: $(which python3)"

# Check permissions for converter
if [ ! -x "$CONVERTER_SCRIPT" ]; then
    echo "[FIX] Making converter script executable..."
    chmod +x "$CONVERTER_SCRIPT"
fi

# ==============================================================================
# STEP 2: SETUP & DOWNLOAD
# ==============================================================================
print_header "STEP 2: Setting up Test Environment"
mkdir -p "$DATA_DIR"

download_if_missing "$ID_BAG_0" "$DATA_DIR/test_0.bag"
download_if_missing "$ID_BAG_1" "$DATA_DIR/test_1.bag"
download_if_missing "$ID_BAG_2" "$DATA_DIR/test_2.bag"
download_if_missing "$ID_LAYOUT" "$DATA_DIR/$LAYOUT_FILE"

# ==============================================================================
# STEP 3: TEST CASE: Single File Conversion
# ==============================================================================
print_header "STEP 3: Testing Single File Conversion"

OUT_SINGLE="$DATA_DIR/output_single"
echo "[INFO] Target Output: $OUT_SINGLE"

# Turn on echo for the command
set -x
$CONVERTER_SCRIPT "$DATA_DIR/test_0.bag" --out-dir "$OUT_SINGLE"
set +x

if [ -f "$OUT_SINGLE/test_0.mcap" ]; then
    echo "✅ Single file conversion successful."
else
    echo "❌ Failed. File not found: $OUT_SINGLE/test_0.mcap"
    exit 1
fi

# ==============================================================================
# STEP 4: TEST CASE: Partial Sequence
# ==============================================================================
print_header "STEP 4: Testing File List Sequence (test_1 + test_2)"

OUT_SEQ="$DATA_DIR/output_sequence"
echo "[INFO] Target Output: $OUT_SEQ"

set -x
$CONVERTER_SCRIPT "$DATA_DIR/test_1.bag" "$DATA_DIR/test_2.bag" --series --out-dir "$OUT_SEQ"
set +x

if [ -f "$OUT_SEQ/test_1.mcap" ] && [ -f "$OUT_SEQ/test_2.mcap" ]; then
    echo "✅ List sequence conversion successful."
else
    echo "❌ Failed. Files missing in: $OUT_SEQ"
    exit 1
fi

# ==============================================================================
# STEP 5: TEST CASE: Full Folder Conversion
# ==============================================================================
print_header "STEP 5: Testing Full Folder Conversion"

OUT_FOLDER="$DATA_DIR/output_folder"
echo "[INFO] Target Output: $OUT_FOLDER"

set -x
$CONVERTER_SCRIPT "$DATA_DIR" --series --out-dir "$OUT_FOLDER"
set +x

if [ -f "$OUT_FOLDER/test_0.mcap" ] && [ -f "$OUT_FOLDER/test_1.mcap" ]; then
    echo "✅ Folder conversion successful."
    echo "   Final Output located at: $OUT_FOLDER"
else
    echo "❌ Failed. Output missing in: $OUT_FOLDER"
    exit 1
fi

# ==============================================================================
# STEP 6: VISUALIZATION
# ==============================================================================
print_header "STEP 6: Launching Foxglove Studio"
echo "Instructions:"
echo "1. Import layout: '$SCRIPT_DIR/$DATA_DIR/$LAYOUT_FILE'"
echo "2. Drag the folder '$SCRIPT_DIR/$OUT_FOLDER' into Foxglove."


# ==============================================================================
# STEP 7: TEST CASE: Splitting by Size
# ==============================================================================
print_header "STEP 7: Testing Split Size (Limit 50MB)" # CHANGED from 200MB

OUT_SPLIT="$DATA_DIR/output_split"
echo "[INFO] Target Output: $OUT_SPLIT"

# Clean up previous failed run if exists
rm -rf "$OUT_SPLIT"

set -x
$CONVERTER_SCRIPT "$DATA_DIR/test_0.bag" --out-dir "$OUT_SPLIT" --split-size 50M
set +x

if [ -f "$OUT_SPLIT/test_0.mcap" ] && [ -f "$OUT_SPLIT/test_0_1.mcap" ]; then
    echo "✅ Split conversion successful."
    ls -lh "$OUT_SPLIT"
else
    echo "❌ Failed. Split files missing in: $OUT_SPLIT"
    exit 1
fi

if command -v foxglove-studio &> /dev/null; then
    # Open the first file to trigger the app, but drag-and-drop folder is best for sequence
    foxglove-studio "$OUT_FOLDER/test_1.mcap" &
else
    echo "[INFO] Foxglove not found in PATH. Please open manually."
fi

echo "Done!"