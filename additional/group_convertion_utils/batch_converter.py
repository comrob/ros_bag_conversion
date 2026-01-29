import os
import argparse
import subprocess
import socket
import time
import urllib.request
from pathlib import Path

# --- Notification Utility ---
def send_ntfy(topic, message, title="Batch Converter", priority="default", tags=None):
    if not topic:
        return

    url = f"https://ntfy.sh/{topic}"
    hostname = socket.gethostname()
    
    headers = {
        "Title": title,
        "Priority": priority,
        "Tags": ",".join(tags) if tags else ""
    }
    
    full_message = f"[{hostname}] {message}"
    
    try:
        req = urllib.request.Request(
            url, 
            data=full_message.encode('utf-8'), 
            headers=headers, 
            method='POST'
        )
        with urllib.request.urlopen(req) as response:
            pass 
    except Exception as e:
        print(f"[WARN] Failed to send ntfy notification: {e}")

def get_folder_bag_stats(folder_path):
    """Calculates total size of bag files in folder."""
    total_size = 0
    bag_count = 0
    for f in folder_path.glob("*.bag"):
        total_size += f.stat().st_size
        bag_count += 1
    
    for unit in ['B', 'KB', 'MB', 'GB']:
        if total_size < 1024.0:
            break
        total_size /= 1024.0
    return bag_count, f"{total_size:.2f} {unit}"

# --- Core Logic ---
def run_batch_conversion(src_root, dst_root, converter_cmd, ntfy_topic=None, dry_run=False, with_plugins=False):
    src_root = Path(src_root).resolve()
    dst_root = Path(dst_root).resolve()
    
    if not src_root.exists():
        print(f"[Error] Source directory not found: {src_root}")
        return

    tasks = []
    for dirpath, dirnames, filenames in os.walk(src_root):
        if any(f.endswith('.bag') for f in filenames):
            tasks.append(Path(dirpath))

    total_tasks = len(tasks)
    print(f"Found {total_tasks} folders to process.")

    if not dry_run:
        send_ntfy(ntfy_topic, f"Started batch conversion of {total_tasks} folders.", title="Batch Started", tags=["rocket"])

    start_time = time.time()
    success_count = 0
    fail_count = 0
    skipped_count = 0

    for index, current_input_path in enumerate(tasks, 1):
        relative_path = current_input_path.relative_to(src_root)
        target_output_folder = dst_root / relative_path
        
        is_done = (target_output_folder / "metadata.yaml").exists()

        if is_done:
            print(f"[{index}/{total_tasks}] [SKIP] {relative_path}")
            skipped_count += 1
            continue

        print(f"\n[{index}/{total_tasks}] [Processing] {relative_path}")
        
        cmd = [
            converter_cmd,
            str(current_input_path),
            "--series",
            "--out-dir", str(target_output_folder)
        ]

        if with_plugins:
            cmd.append("--with-plugins")

        if dry_run:
            print(f"  [Dry Run] CMD: {' '.join(cmd)}")
            success_count += 1 
        else:
            try:
                target_output_folder.parent.mkdir(parents=True, exist_ok=True)
                
                t0 = time.time()
                
                # Capture output to check for specific warnings
                result = subprocess.run(
                    cmd, 
                    check=True, 
                    capture_output=True, 
                    text=True
                )
                
                # Print stdout to console so we can see it live
                if result.stdout: print(result.stdout, end='')
                if result.stderr: print(result.stderr, end='')

                duration = time.time() - t0
                success_count += 1
                
                # --- CHECK FOR "EMPTY BAG" WARNING ---
                if "[WARN] Bag duration is" in result.stdout:
                    bag_count, size_str = get_folder_bag_stats(current_input_path)
                    
                    warn_msg = (
                        f"Processed Empty/Instant Bag: {relative_path}\n"
                        f"Size: {size_str} ({bag_count} bags)"
                    )
                    # Send High priority Warning
                    send_ntfy(ntfy_topic, warn_msg, title="Empty Bag Detected", priority="high", tags=["warning"])
                
            except subprocess.CalledProcessError as e:
                fail_count += 1
                
                # Stats
                bag_count, size_str = get_folder_bag_stats(current_input_path)
                
                # Extract error
                err_lines = e.stderr.strip().split('\n')
                last_err = err_lines[-1] if err_lines else "Unknown Error"
                if len(err_lines) > 1:
                     last_err = f"{err_lines[-2]}\n{err_lines[-1]}"

                error_msg = (
                    f"Failed: {relative_path}\n"
                    f"Size: {size_str} ({bag_count} bags)\n"
                    f"Error: {last_err}"
                )
                
                print(f"  [ERROR] Exit Code: {e.returncode}")
                print(e.stderr)
                
                send_ntfy(ntfy_topic, error_msg, title="Conversion Error", priority="high", tags=["x"])
                
            except Exception as e:
                fail_count += 1
                error_msg = f"Critical Error on {relative_path}\n{str(e)}"
                print(f"  [ERROR] {error_msg}")
                send_ntfy(ntfy_topic, error_msg, title="Critical Failure", priority="urgent", tags=["skull"])

    total_duration = time.time() - start_time
    summary = (
        f"Batch Finished in {total_duration/60:.1f} min.\n"
        f"Total: {total_tasks}\n"
        f"Success: {success_count}\n"
        f"Skipped: {skipped_count}\n"
        f"Failed: {fail_count}"
    )
    
    print("\n" + "=" * 60)
    print(summary)
    print("=" * 60)

    if not dry_run:
        tag = "tada" if fail_count == 0 else "warning"
        send_ntfy(ntfy_topic, summary, title="Batch Complete", priority="default", tags=[tag])

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Smart Batch Convert ROS1 -> MCAP")
    parser.add_argument("--input", required=True, help="Root folder containing ROS1 bag folders")
    parser.add_argument("--output", required=True, help="Root folder for MCAP output")
    parser.add_argument("--converter", default="convert_bag", help="Command to run converter")
    parser.add_argument("--with-plugins", action="store_true", help="Enable plugins")
    parser.add_argument("--dry-run", action="store_true", help="Print commands without running")
    parser.add_argument("--ntfy", default=None, help="ntfy.sh topic")

    args = parser.parse_args()

    run_batch_conversion(
        args.input, 
        args.output, 
        args.converter, 
        ntfy_topic=args.ntfy,
        dry_run=args.dry_run, 
        with_plugins=args.with_plugins
    )