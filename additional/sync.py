#!/usr/bin/env python3
import sys
import os
import time
import argparse
import subprocess
import urllib.request

# --- DEFAULTS ---
DEFAULT_NTFY_TOPIC = "secret_topic_for_sync_notifications"

class SyncManager:
    def __init__(self, source, dest, dry_run=False, ntfy_topic=None):
        self.source = source 
        self.dest = dest if dest.endswith('/') else dest + '/'
        self.dry_run = dry_run
        self.total_files = 0
        self.current_count = 0
        
        topic = ntfy_topic if ntfy_topic else DEFAULT_NTFY_TOPIC
        self.ntfy_url = f"https://ntfy.sh/{topic}"
        print(f"[INFO] Notifications will be sent to: {self.ntfy_url}")

    def send_alert(self, title, message, tags):
        """Sends notification. Prefixes title if in Dry Run."""
        # [MODIFIED] Logic to allow dry-run notifications
        prefix = "[DRY RUN] " if self.dry_run else ""
        final_title = f"{prefix}{title}"

        try:
            headers = {"Title": final_title, "Priority": "default", "Tags": tags}
            data = message.encode('utf-8')
            req = urllib.request.Request(self.ntfy_url, data=data, headers=headers, method="POST")
            with urllib.request.urlopen(req): pass
        except Exception as e:
            print(f"[WARN] Notification failed: {e}")

    def calculate_total(self):
        print("=====================================================")
        print("PHASE 1: CALCULATING DIFFERENCES")
        print("=====================================================")
        
        cmd = ["rsync", "-nai", "--ignore-existing", "--modify-window=1", self.source, self.dest]
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            self.total_files = sum(1 for line in result.stdout.splitlines() if line.startswith('>f'))
            print(f"Files to sync: {self.total_files}")
            
            # [NEW] Notify on start if there is work to do
            if self.total_files > 0:
                self.send_alert("Sync Started", f"Found {self.total_files} files to process.", "rocket")
            
            return self.total_files
        except subprocess.CalledProcessError as e:
            print(f"[ERR] Failed to calculate: {e}")
            sys.exit(1)

    def run_sync(self):
        print("\n-----------------------------------------------------")
        print("PHASE 2: EXECUTION")
        
        if self.dry_run:
            print("*** DRY RUN MODE - PREVIEWING DESTINATIONS ***")
        else:
            print("Starting in 3 seconds...")
            time.sleep(3)

        base_cmd = [
            "rsync", "-avP", "--timeout=60", "--modify-window=1", 
            "--ignore-existing", "--partial-dir=.rsync-partial",
            "--out-format=TRANSFER_ID:%n", 
            self.source, self.dest
        ]
        if self.dry_run: base_cmd.insert(1, "-n")

        while True:
            process = subprocess.Popen(base_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
            
            try:
                for line in process.stdout:
                    if "TRANSFER_ID:" in line:
                        rel_path = line.strip().split("TRANSFER_ID:", 1)[1]
                        
                        if not rel_path.endswith('/'):
                            self.current_count += 1
                            
                            full_dest_path = os.path.join(self.dest, rel_path)
                            
                            if self.dry_run:
                                # [MODIFIED] Print only, NO per-file notification in dry-run to avoid spam
                                print(f"[Will Copy] {rel_path}  -->  {full_dest_path}")
                            else:
                                print(line, end='') 
                                self.send_alert("Upload Progress", f"[{self.current_count}/{self.total_files}] {rel_path}", "package")
                    elif not self.dry_run:
                        print(line, end='') 
            except Exception:
                process.kill()
                raise

            if process.wait() == 0:
                print("\nSync Successful!")
                # [MODIFIED] Always send completion alert, even in dry run
                self.send_alert("Backup Complete", f"✅ Processed {self.current_count} files.", "white_check_mark")
                break
            else:
                if self.dry_run: 
                    self.send_alert("Backup Failed", f"❌ Dry run failed with code {process.returncode}", "x")
                    break 
                
                print(f"\n[WARN] Retrying in 10s...")
                self.send_alert("Backup Warning", f"⚠️ Interrupted. Retrying...", "warning")
                time.sleep(10)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("source", help="Source directory")
    parser.add_argument("dest", help="Destination directory")
    parser.add_argument("-d", "--dry-run", action="store_true", help="Simulate transfer")
    parser.add_argument("--topic", default=DEFAULT_NTFY_TOPIC, help="Override NTFY topic")
    
    args = parser.parse_args()

    if not os.path.exists(args.source):
        print(f"Error: Source '{args.source}' not found.")
        sys.exit(1)

    manager = SyncManager(args.source, args.dest, args.dry_run, args.topic)
    
    count = manager.calculate_total()
    
    if count > 0:
        manager.run_sync()
    else:
        print("Source and Destination are already in sync. Nothing to do.")