import sys
import re
import argparse
import signal
import yaml
from pathlib import Path
from typing import List, Dict, Optional, Any
import numpy as np

# ROS1 Reader
from rosbags.rosbag1 import Reader
# Type System
from rosbags.typesys import get_types_from_msg, get_typestore, Stores

# MCAP Writer
from mcap.writer import Writer as McapWriter
from mcap.well_known import SchemaEncoding, MessageEncoding, Profile

# --- Signal Handler for Graceful Exit ---
class GracefulExiter:
    def __init__(self):
        self.stop_requested = False
        signal.signal(signal.SIGINT, self.request_stop)
        signal.signal(signal.SIGTERM, self.request_stop)

    def request_stop(self, *args):
        if not self.stop_requested:
            print("\n[WARN] Stop requested! Finishing current message and closing file safely...")
            print("[TIP]  Please wait a moment for the footer to write.")
            self.stop_requested = True
        else:
            print("\n[ALERT] Force kill detected. File may be corrupted.")
            sys.exit(1)

# --- Configuration & Constants ---
TF_STATIC_TOPIC = '/tf_static'
TF_STATIC_QOS_POLICY = """
- history: 3
  depth: 1
  reliability: 1
  durability: 1
  deadline: {sec: 2147483647, nsec: 4294967295}
  lifespan: {sec: 2147483647, nsec: 4294967295}
  liveliness: 1
  liveliness_lease_duration: {sec: 2147483647, nsec: 4294967295}
  avoid_ros_namespace_conventions: false
""".strip()

# --- Helper Functions ---

def to_ros2_type(ros1_type: str) -> str:
    if ros1_type == 'Header': return 'std_msgs/msg/Header'
    if "/msg/" not in ros1_type and "/" in ros1_type:
        parts = ros1_type.split('/')
        if len(parts) == 2:
            return f"{parts[0]}/msg/{parts[1]}"
    return ros1_type

def fix_ros1_def(text: str, type_name: str) -> str:
    """Parses ROS1 definitions line-by-line to safely convert types to ROS2."""
    if "CameraInfo" in type_name:
        text = text.replace("float64[] D", "float64[] d")
        text = text.replace("float64[9] K", "float64[9] k")
        text = text.replace("float64[9] R", "float64[9] r")
        text = text.replace("float64[12] P", "float64[12] p")

    lines = text.splitlines()
    fixed_lines = []
    
    for line in lines:
        stripped = line.strip()
        if not stripped:
            fixed_lines.append(line)
            continue
        if stripped.startswith("uint32 seq"): continue
        if stripped.startswith("MSG:"):
            line = re.sub(r'(^MSG:\s+)([a-zA-Z0-9_]+)/(?!msg/)([a-zA-Z0-9_]+)', r'\1\2/msg/\3', line)
            fixed_lines.append(line)
            continue

        match = re.match(r'^(\s*)([a-zA-Z0-9_/\[\]]+)(\s+)([^#\s]+)(.*)$', line)
        if match:
            indent, type_str, ws, name_str, rest = match.groups()
            
            if type_str == 'time': type_str = 'builtin_interfaces/msg/Time'
            elif type_str == 'duration': type_str = 'builtin_interfaces/msg/Duration'
            elif type_str == 'Header': type_str = 'std_msgs/msg/Header'
            
            if '/' in type_str and '/msg/' not in type_str:
                base_type = type_str.split('[')[0] 
                suffix = type_str[len(base_type):] 
                parts = base_type.split('/')
                if len(parts) == 2:
                    type_str = f"{parts[0]}/msg/{parts[1]}{suffix}"

            if "CameraInfo" in type_name:
                if name_str in ['D', 'K', 'R', 'P']: name_str = name_str.lower()

            new_line = f"{indent}{type_str}{ws}{name_str}{rest}"
            fixed_lines.append(new_line)
        else:
            fixed_lines.append(line)

    return "\n".join(fixed_lines)

def convert_ros1_to_ros2(ros1_obj, ros2_type, ros2_store):
    try:
        MsgType = ros2_store.types[ros2_type]
    except KeyError:
        return ros1_obj

    kwargs = {}
    for field_name, field_desc in MsgType.__dataclass_fields__.items():
        val = getattr(ros1_obj, field_name, None)
        if val is None:
            val = getattr(ros1_obj, field_name.upper(), None)

        if val is None:
            if field_name in ['d', 'k', 'r', 'p', 'binning_x', 'binning_y', 'covariance']:
                size = 0
                if field_name in ['k', 'r', 'covariance']: size = 9
                elif field_name == 'p': size = 12
                val = np.zeros(size, dtype=np.float64)
            else:
                continue

        if isinstance(val, (list, tuple)):
            if field_name in ['d', 'k', 'r', 'p', 'covariance'] or \
               (len(val) > 0 and isinstance(val[0], (int, float))):
                np_val = np.array(val, dtype=np.float64)
                expected_size = 0
                if field_name in ['k', 'r', 'covariance']: expected_size = 9
                elif field_name == 'p': expected_size = 12
                if expected_size > 0 and np_val.size != expected_size:
                    new_arr = np.zeros(expected_size, dtype=np.float64)
                    lim = min(np_val.size, expected_size)
                    new_arr[:lim] = np_val[:lim]
                    val = new_arr
                else:
                    val = np_val

        if hasattr(val, '__msgtype__'):
            sub_type_ros1 = val.__msgtype__
            sub_type_ros2 = to_ros2_type(sub_type_ros1)
            val = convert_ros1_to_ros2(val, sub_type_ros2, ros2_store)
        elif isinstance(val, list):
             if len(val) > 0 and hasattr(val[0], '__msgtype__'):
                 sub_type_ros1 = val[0].__msgtype__
                 sub_type_ros2 = to_ros2_type(sub_type_ros1)
                 val = [convert_ros1_to_ros2(x, sub_type_ros2, ros2_store) for x in val]
        
        if hasattr(val, 'secs') and hasattr(val, 'nsecs'):
             if 'builtin_interfaces/msg/Time' in str(field_desc.type):
                 from rosbags.typesys.stores.ros2_humble import builtin_interfaces__msg__Time as Time
                 val = Time(sec=val.secs, nanosec=val.nsecs)
             elif 'builtin_interfaces/msg/Duration' in str(field_desc.type):
                 from rosbags.typesys.stores.ros2_humble import builtin_interfaces__msg__Duration as Duration
                 val = Duration(sec=val.secs, nanosec=val.nsecs)

        kwargs[field_name] = val
    return MsgType(**kwargs)

# --- Metadata Logic ---

def write_metadata_yaml(folder: Path, stats_list: List[Dict], distro: str):
    """
    Generates rosbag2 compliant metadata.yaml from collected stats.
    Robust against failures to ensure MCAP files are safe.
    """
    try:
        if not stats_list:
            return

        print(f"[INFO] Generating metadata.yaml for {len(stats_list)} files...")

        # Aggregation Variables
        total_messages = 0
        min_start = None
        max_end = None
        
        # Topic aggregation: {topic_name: {count: int, type: str, serialization: str}}
        merged_topics = {}

        # Files list for YAML
        files_data = []

        for s in stats_list:
            # 1. Aggregate Time
            if min_start is None or s['start_time'] < min_start:
                min_start = s['start_time']
            if max_end is None or s['end_time'] > max_end:
                max_end = s['end_time']
            
            # 2. Aggregate Total Count
            total_messages += s['message_count']

            # 3. Aggregate Topics
            for t_name, t_info in s['topics'].items():
                if t_name not in merged_topics:
                    merged_topics[t_name] = t_info.copy()
                else:
                    merged_topics[t_name]['count'] += t_info['count']

            # 4. Per-File Entry
            files_data.append({
                "path": s['filename'],
                "starting_time": {"nanoseconds_since_epoch": s['start_time']},
                "duration": {"nanoseconds": s['duration']},
                "message_count": s['message_count']
            })

        # Calculate Globals
        duration_ns = (max_end - min_start) if (max_end and min_start) else 0

        # Format Topics List
        topics_list = []
        for t_name, t_data in merged_topics.items():
            topics_list.append({
                "topic_metadata": {
                    "name": t_name,
                    "type": t_data['type'],
                    "serialization_format": "cdr",
                    "offered_qos_profiles": ""  # Simplify for compatibility
                },
                "message_count": t_data['count']
            })

        # Construct YAML Object
        metadata = {
            "rosbag2_bagfile_information": {
                "version": 5,
                "storage_identifier": "mcap",
                "ros_distro": distro,
                "relative_file_paths": [f['path'] for f in files_data],
                "duration": {"nanoseconds": duration_ns},
                "starting_time": {"nanoseconds_since_epoch": min_start if min_start else 0},
                "message_count": total_messages,
                "topics_with_message_count": topics_list,
                "compression_format": "",
                "compression_mode": "",
                "files": files_data
            }
        }

        # Write to File
        yaml_path = folder / "metadata.yaml"
        with open(yaml_path, "w") as f:
            yaml.dump(metadata, f, default_flow_style=False)
        
        print(f"[SUCCESS] Metadata written to {yaml_path}")

    except Exception as e:
        print(f"[ERR] Failed to generate metadata.yaml: {e}")
        print("[TIP] MCAP files are still valid, but 'ros2 bag info' on the folder might fail.")


# --- Business Logic / Series Handling ---

class BagSeriesConverter:
    def __init__(self, ros_distro="humble"):
        self.ros_distro = ros_distro
        self.static_tf_cache: List[Any] = []
        self.tf_static_def: Optional[str] = None 
        
        print(f"[INFO] Initializing Stores...")
        self.store_ros1 = get_typestore(Stores.ROS1_NOETIC)
        self.store_ros2 = get_typestore(Stores.ROS2_HUMBLE)
        self.exiter = GracefulExiter()

    def convert_file(self, src: Path, dst: Path, is_part_of_series: bool = False) -> Optional[Dict]:
        """
        Returns stats dict if successful, None if failed/empty.
        Stats: {filename, start_time, end_time, duration, message_count, topics}
        """
        print(f"[INFO] Processing: {src.name} -> {dst.name}")
        
        tf_type_name = "tf2_msgs/msg/TFMessage"
        
        # Stats containers
        stats_topics = {} # {topic: {count, type}}
        stats_start = None
        stats_end = None
        stats_count = 0

        with Reader(src) as reader:
            with open(dst, "wb") as f_out:
                writer = McapWriter(f_out)
                writer.start(profile=Profile.ROS2) 
                
                topic_to_chan_id = {}
                conn_map = {}
                type_map = {}
                registered_schemas = {}
                
                try:
                    # --- PASS 1: REGISTRATION ---
                    for conn in reader.connections:
                        if self.exiter.stop_requested: break

                        msg_def = None
                        if hasattr(conn, 'msgdef'): msg_def = conn.msgdef
                        elif hasattr(conn, 'ext'):
                             if isinstance(conn.ext, dict): msg_def = conn.ext.get('message_definition', None)
                             elif hasattr(conn.ext, 'message_definition'): msg_def = conn.ext.message_definition
                        if not msg_def and conn.msgtype in reader.msgtypes: msg_def = reader.msgtypes[conn.msgtype]
                        if not msg_def: continue

                        ros1_type = conn.msgtype
                        ros2_type = to_ros2_type(ros1_type)
                        
                        try:
                            self.store_ros1.register(get_types_from_msg(msg_def, ros1_type))
                        except: pass
                        
                        fixed_def = fix_ros1_def(msg_def, ros2_type)
                        try:
                            types_v2 = get_types_from_msg(fixed_def, ros2_type)
                            self.store_ros2.register(types_v2)
                        except Exception as e:
                            print(f"  [WARN] Type register fail {ros2_type}: {e}")

                        if ros2_type not in registered_schemas:
                            sid = writer.register_schema(name=ros2_type, encoding=SchemaEncoding.ROS2, data=fixed_def.encode('utf-8'))
                            registered_schemas[ros2_type] = sid
                            
                            if conn.topic == TF_STATIC_TOPIC:
                                self.tf_static_def = fixed_def
                        
                        if conn.topic not in topic_to_chan_id:
                            meta = {}
                            if conn.topic == TF_STATIC_TOPIC:
                                meta["offered_qos_profiles"] = TF_STATIC_QOS_POLICY
                            
                            cid = writer.register_channel(
                                topic=conn.topic,
                                message_encoding=MessageEncoding.CDR,
                                schema_id=registered_schemas[ros2_type],
                                metadata=meta
                            )
                            topic_to_chan_id[conn.topic] = cid
                            stats_topics[conn.topic] = {"count": 0, "type": ros2_type}

                        conn_map[conn.id] = topic_to_chan_id[conn.topic]
                        type_map[conn.id] = ros2_type

                    # --- INJECTION LOGIC ---
                    if is_part_of_series and self.static_tf_cache and not self.exiter.stop_requested:
                        chan_id = 0
                        schema_id_to_use = 0

                        if tf_type_name in registered_schemas:
                            schema_id_to_use = registered_schemas[tf_type_name]
                        elif self.tf_static_def:
                            print(f"  [INFO] Registering missing schema for {tf_type_name}...")
                            schema_id_to_use = writer.register_schema(
                                name=tf_type_name, 
                                encoding=SchemaEncoding.ROS2, 
                                data=self.tf_static_def.encode('utf-8')
                            )
                            registered_schemas[tf_type_name] = schema_id_to_use

                        if schema_id_to_use != 0:
                            if TF_STATIC_TOPIC in topic_to_chan_id:
                                chan_id = topic_to_chan_id[TF_STATIC_TOPIC]
                            else:
                                print(f"  [INFO] Creating missing {TF_STATIC_TOPIC} channel...")
                                meta = {"offered_qos_profiles": TF_STATIC_QOS_POLICY}
                                chan_id = writer.register_channel(
                                    topic=TF_STATIC_TOPIC,
                                    message_encoding=MessageEncoding.CDR,
                                    schema_id=schema_id_to_use,
                                    metadata=meta
                                )
                                topic_to_chan_id[TF_STATIC_TOPIC] = chan_id
                                stats_topics[TF_STATIC_TOPIC] = {"count": 0, "type": tf_type_name}
                        
                        if chan_id != 0:
                            print(f"  [INFO] Injecting {len(self.static_tf_cache)} static transforms...")
                            start_time = reader.start_time if reader.start_time else 0
                            
                            from rosbags.typesys.stores.ros2_humble import builtin_interfaces__msg__Time as Time
                            
                            for ros2_msg in self.static_tf_cache:
                                if hasattr(ros2_msg, 'transforms'):
                                    for t in ros2_msg.transforms:
                                        t.header.stamp = Time(sec=int(start_time/1e9), nanosec=int(start_time%1e9))

                                cdr_bytes = self.store_ros2.serialize_cdr(ros2_msg, tf_type_name)
                                writer.add_message(channel_id=chan_id, log_time=start_time, publish_time=start_time, data=cdr_bytes)
                                
                                # Stats Update
                                stats_topics[TF_STATIC_TOPIC]["count"] += 1
                                stats_count += 1
                                if stats_start is None or start_time < stats_start: stats_start = start_time
                                if stats_end is None or start_time > stats_end: stats_end = start_time

                    # --- PASS 2: CONVERSION ---
                    print("  [INFO] Converting messages... (Press Ctrl+C to stop safely)")
                    
                    for conn, timestamp, raw_data in reader.messages():
                        if self.exiter.stop_requested: break
                        if conn.id not in conn_map: continue
                        
                        ros2_t = type_map[conn.id]
                        
                        try:
                            ros1_msg = self.store_ros1.deserialize_ros1(raw_data, conn.msgtype)
                            ros2_msg = convert_ros1_to_ros2(ros1_msg, ros2_t, self.store_ros2)
                            cdr_bytes = self.store_ros2.serialize_cdr(ros2_msg, ros2_t)

                            writer.add_message(
                                channel_id=conn_map[conn.id],
                                log_time=timestamp,
                                publish_time=timestamp,
                                data=cdr_bytes
                            )

                            if conn.topic == TF_STATIC_TOPIC:
                                self.static_tf_cache.append(ros2_msg)
                            
                            # Stats Update
                            stats_topics[conn.topic]["count"] += 1
                            stats_count += 1
                            if stats_start is None or timestamp < stats_start: stats_start = timestamp
                            if stats_end is None or timestamp > stats_end: stats_end = timestamp

                            if stats_count % 10000 == 0: print(f"         {stats_count}...", end='\r')
                        except Exception: pass

                    print(f"\n  [SUCCESS] {stats_count} messages converted.")

                finally:
                    writer.finish()
                    print(f"[INFO] File finalized: {dst}")

        # Return Stats for Aggregation
        if stats_start is None: stats_start = 0
        if stats_end is None: stats_end = 0
        
        return {
            "filename": dst.name,
            "start_time": int(stats_start),
            "end_time": int(stats_end),
            "duration": int(stats_end - stats_start),
            "message_count": stats_count,
            "topics": stats_topics
        }

# --- Main Entry Point ---

def main():
    parser = argparse.ArgumentParser(description="Convert ROS1 bags to MCAP.")
    parser.add_argument("inputs", nargs='+', help="Input .bag files or a folder")
    parser.add_argument("--series", action="store_true", help="Treat inputs as a split sequence")
    parser.add_argument("--distro", default="humble", help="ROS distro for metadata")
    parser.add_argument("--out-dir", type=Path, default=None, help="Force a specific output directory")

    args = parser.parse_args()
    
    # --- 1. Resolve Inputs ---
    input_paths = [Path(p) for p in args.inputs]
    bag_files = []
    is_folder_input = False
    
    if len(input_paths) == 1 and input_paths[0].is_dir():
        is_folder_input = True
        root_folder = input_paths[0]
        bag_files = sorted(list(root_folder.glob("*.bag")))
        print(f"[INFO] Detected folder input: {root_folder} ({len(bag_files)} bags)")
    else:
        bag_files = sorted([p for p in input_paths if p.suffix == ".bag"])

    if not bag_files:
        print("[ERR] No .bag files found.")
        sys.exit(1)

    print(f"[INFO] Processing {len(bag_files)} files. Series Mode: {args.series}")
    
    # --- 2. Determine Output Directory ---
    output_dir = args.out_dir
    
    if output_dir is None:
        first_file = bag_files[0]
        if args.series:
            if is_folder_input:
                parent_name = first_file.parent.name
                output_dir = first_file.parent.parent / f"{parent_name}_mcap"
            else:
                stem = first_file.stem
                output_dir = first_file.parent / f"{stem}_series_mcap"
            print(f"[INFO] Output Directory: {output_dir}")
            output_dir.mkdir(parents=True, exist_ok=True)
        else:
            output_dir = None

    # --- 3. Execution ---
    converter = BagSeriesConverter(ros_distro=args.distro)
    
    # Store stats for valid files to generate metadata later
    valid_stats = []

    for f in bag_files:
        if converter.exiter.stop_requested:
            print("[INFO] Batch processing stopped.")
            break

        if output_dir:
            dst = output_dir / f.with_suffix(".mcap").name
        else:
            dst = f.with_suffix(".mcap")

        # Convert and capture stats
        stats = converter.convert_file(f, dst, is_part_of_series=args.series)
        if stats:
            valid_stats.append(stats)
        
        if not args.series:
            converter.static_tf_cache.clear()
            converter.tf_static_def = None

    # --- 4. Generate Metadata (If Series/Folder Output) ---
    # Only generate if we have an output directory (implies series/folder mode)
    if output_dir and valid_stats:
        write_metadata_yaml(output_dir, valid_stats, args.distro)

if __name__ == "__main__":
    main()