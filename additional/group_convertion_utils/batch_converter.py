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
        req = urllib.request.Request(url, data=full_message.encode('utf-8'), headers=headers, method='POST')
        with urllib.request.urlopen(req) as response:
            pass 
    except Exception as e:
        print(f"[WARN] Failed to send ntfy notification: {e}")

def get_folder_bag_stats(folder_path):
    total_size = 0
    bag_count = 0
    for f in folder_path.glob("*.bag"):
        total_size += f.stat().st_size
        bag_count += 1
    unit = 'B'
    for u in ['B', 'KB', 'MB', 'GB']:
        unit = u
        if total_size < 1024.0:
            break
        total_size /= 1024.0
    return bag_count, f"{total_size:.2f} {unit}"

# --- Core Logic ---
def run_batch_conversion(src_root, dst_root, converter_cmd, ntfy_topic=None, **kwargs):
    # Extract dry_run from kwargs to avoid multiple values error
    dry_run = kwargs.get('dry_run', False)
    
    src_root = Path(src_root).resolve()
    dst_root = Path(dst_root).resolve()
    
    if not src_root.exists():
        print(f"[Error] Source directory not found: {src_root}")
        return

    tasks = []
    for dirpath, _, filenames in os.walk(src_root):
        if any(f.endswith('.bag') for f in filenames):
            tasks.append(Path(dirpath))

    total_tasks = len(tasks)
    print(f"Found {total_tasks} folders to process.")

    if not dry_run:
        send_ntfy(ntfy_topic, f"Started batch conversion of {total_tasks} folders.", title="Batch Started", tags=["rocket"])

    start_time = time.time()
    success_count, fail_count, skipped_count = 0, 0, 0

    for index, current_input_path in enumerate(tasks, 1):
        relative_path = current_input_path.relative_to(src_root)
        target_output_folder = dst_root / relative_path
        
        if (target_output_folder / "metadata.yaml").exists():
            print(f"[{index}/{total_tasks}] [SKIP] {relative_path}")
            skipped_count += 1
            continue

        print(f"\n[{index}/{total_tasks}] [Processing] {relative_path}")
        
        # Build command dynamically
        cmd = [converter_cmd, str(current_input_path)]
        
        # Forward arguments to the underlying converter
        if kwargs.get('series'): cmd.append("--series")
        if kwargs.get('with_plugins'): cmd.append("--with-plugins")
        if kwargs.get('split_size'): cmd.extend(["--split-size", kwargs['split_size']])
        if kwargs.get('distro'): cmd.extend(["--distro", kwargs['distro']])
        if dry_run: cmd.append("--dry-run")
        
        cmd.extend(["--out-dir", str(target_output_folder)])

        print(f"  [EXEC] {' '.join(cmd)}")

        if dry_run:
            success_count += 1 
            print("  [Dry Run] No action taken.")
        else:
            try:
                target_output_folder.parent.mkdir(parents=True, exist_ok=True)
                t0 = time.time()
                
                # Execute and capture output
                result = subprocess.run(cmd, check=True, capture_output=True, text=True)
                
                if result.stdout: print(result.stdout, end='')
                if result.stderr: print(result.stderr, end='')

                duration = time.time() - t0
                success_count += 1
                
                send_ntfy(
                    ntfy_topic, 
                    f"Converted: {relative_path}\nDuration: {duration:.1f}s", 
                    title=f"File {index}/{total_tasks} Success", 
                    tags=["white_check_mark"]
                )
                
                if "[WARN] Bag duration is" in result.stdout:
                    bag_count, size_str = get_folder_bag_stats(current_input_path)
                    send_ntfy(ntfy_topic, f"Empty Bag: {relative_path}\n{size_str}", title="Warning", priority="high", tags=["warning"])
                
            except subprocess.CalledProcessError as e:
                fail_count += 1
                bag_count, size_str = get_folder_bag_stats(current_input_path)
                # Show the last line of stderr in the alert
                err_summary = e.stderr.strip().splitlines()[-1] if e.stderr else "Subprocess failed"
                send_ntfy(ntfy_topic, f"Failed: {relative_path}\n{size_str}\n{err_summary}", title="Conversion Error", priority="high", tags=["x"])
                print(f"  [ERROR] {e.stderr}")
                
            except Exception as e:
                fail_count += 1
                send_ntfy(ntfy_topic, f"Critical Error: {str(e)}", title="Critical Failure", priority="urgent", tags=["skull"])

    total_duration = time.time() - start_time
    summary = f"Finished in {total_duration/60:.1f}m\nSuccess: {success_count}\nFail: {fail_count}"
    print(f"\n{'='*30}\n{summary}\n{'='*30}")

    if not dry_run:
        send_ntfy(ntfy_topic, summary, title="Batch Complete", tags=["tada" if fail_count == 0 else "warning"])

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Smart Batch Convert ROS1 -> MCAP")
    parser.add_argument("--input", required=True, help="Root folder for ROS1 bags")
    parser.add_argument("--output", required=True, help="Root folder for MCAP")
    parser.add_argument("--converter", default="convert_bag", help="Command to run converter")
    parser.add_argument("--ntfy", default=None, help="ntfy.sh topic")
    parser.add_argument("--dry-run", action="store_true", help="Print commands and pass --dry-run to converter")
    
    # Arguments to forward
    parser.add_argument("--series", action="store_true", help="Treat inputs as split sequence")
    parser.add_argument("--split-size", default=None, help="Split output files by size")
    parser.add_argument("--distro", default="humble", help="ROS distro")
    parser.add_argument("--with-plugins", action="store_true", help="Enable plugins")

    args = parser.parse_args()
    
    # Using **vars(args) to pass all parsed arguments to kwargs
    run_batch_conversion(
        args.input, 
        args.output, 
        args.converter, 
        ntfy_topic=args.ntfy, 
        **vars(args)
    )