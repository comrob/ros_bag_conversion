import sys
import argparse
import yaml
import traceback
from pathlib import Path
from typing import List, Dict, Optional, Any

# ROS / MCAP
from rosbags.rosbag1 import Reader
from rosbags.typesys import get_types_from_msg, get_typestore, Stores
from mcap.well_known import SchemaEncoding, MessageEncoding, Profile

# Local Modules
import convert_utils
import writer as mcap_writer_utils

# --- Metadata Logic ---
def write_metadata_yaml(folder: Path, stats_list: List[Dict], distro: str):
    try:
        if not stats_list: return

        print(f"[INFO] Generating metadata.yaml for {len(stats_list)} files...")
        total_messages = 0
        min_start = None
        max_end = None
        merged_topics = {}
        files_data = []

        for s in stats_list:
            if min_start is None or s['start_time'] < min_start: min_start = s['start_time']
            if max_end is None or s['end_time'] > max_end: max_end = s['end_time']
            total_messages += s['message_count']

            for t_name, t_info in s['topics'].items():
                if t_name not in merged_topics:
                    merged_topics[t_name] = t_info.copy()
                else:
                    merged_topics[t_name]['count'] += t_info['count']

            files_data.append({
                "path": s['filename'],
                "starting_time": {"nanoseconds_since_epoch": s['start_time']},
                "duration": {"nanoseconds": s['duration']},
                "message_count": s['message_count']
            })

        duration_ns = (max_end - min_start) if (max_end and min_start) else 0

        topics_list = []
        for t_name, t_data in merged_topics.items():
            topics_list.append({
                "topic_metadata": {
                    "name": t_name,
                    "type": t_data['type'],
                    "serialization_format": "cdr",
                    "offered_qos_profiles": ""
                },
                "message_count": t_data['count']
            })

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

        yaml_path = folder / "metadata.yaml"
        with open(yaml_path, "w") as f:
            yaml.dump(metadata, f, default_flow_style=False)
        print(f"[SUCCESS] Metadata written to {yaml_path}")

    except Exception as e:
        print(f"[ERR] Failed to generate metadata.yaml: {e}")

# --- Business Logic ---
class BagSeriesConverter:
    def __init__(self, ros_distro="humble"):
        self.ros_distro = ros_distro
        self.static_tf_cache: List[Any] = []
        self.tf_static_def: Optional[str] = None 
        
        print(f"[INFO] Initializing Stores...")
        self.store_ros1 = get_typestore(Stores.ROS1_NOETIC)
        self.store_ros2 = get_typestore(Stores.ROS2_HUMBLE)
        self.exiter = convert_utils.GracefulExiter()

    def convert_file(self, src: Path, base_dst: Path, split_size: int = 0, is_part_of_series: bool = False) -> List[Dict]:
        print(f"[INFO] Processing: {src.name}")
        
        tf_type_name = "tf2_msgs/msg/TFMessage"
        all_file_stats = []

        with Reader(src) as reader:
            writer = mcap_writer_utils.RollingMcapWriter(base_dst, split_size, Profile.ROS2)
            
            # Setup first stats object
            current_stats = {
                "filename": writer.current_file_path.name,
                "start_time": None, "end_time": None,
                "message_count": 0, "topics": {}
            }
            
            topic_to_chan_id = {}
            conn_map = {}
            type_map = {}
            registered_schemas = {}
            
            try:
                # --- PASS 1: REGISTRATION ---
                for conn in reader.connections:
                    if self.exiter.stop_requested: break

                    ros1_type = conn.msgtype
                    ros2_type = convert_utils.to_ros2_type(ros1_type)
                    
                    # Optimization: Check if standard type before parsing msgdef
                    is_standard_ros1 = False
                    try:
                        if self.store_ros1.types.get(ros1_type): is_standard_ros1 = True
                    except: pass

                    if not is_standard_ros1:
                        msg_def = None
                        if hasattr(conn, 'msgdef'): msg_def = conn.msgdef
                        elif hasattr(conn, 'ext'):
                             if isinstance(conn.ext, dict): msg_def = conn.ext.get('message_definition', None)
                             elif hasattr(conn.ext, 'message_definition'): msg_def = conn.ext.message_definition
                        if not msg_def and conn.msgtype in reader.msgtypes: msg_def = reader.msgtypes[conn.msgtype]
                        
                        if msg_def:
                            try: self.store_ros1.register(get_types_from_msg(msg_def, ros1_type))
                            except Exception: pass

                            fixed_def = convert_utils.fix_ros1_def(msg_def, ros2_type)
                            try:
                                types_v2 = get_types_from_msg(fixed_def, ros2_type)
                                self.store_ros2.register(types_v2)
                            except Exception as e:
                                if "std_msgs" not in ros1_type and "geometry_msgs" not in ros1_type:
                                    print(f"  [WARN] Type register fail {ros2_type}: {e}")

                    # Get schema definition for MCAP
                    msg_def_for_schema = ""
                    try:
                         if hasattr(conn, 'msgdef'): msg_def_for_schema = conn.msgdef
                    except: pass
                    
                    if not msg_def_for_schema:
                         msg_def_for_schema = f"# {ros2_type}"

                    fixed_def_for_schema = convert_utils.fix_ros1_def(msg_def_for_schema, ros2_type)

                    if ros2_type not in registered_schemas:
                        sid = writer.register_schema(name=ros2_type, encoding=SchemaEncoding.ROS2, data=fixed_def_for_schema.encode('utf-8'))
                        registered_schemas[ros2_type] = sid
                        if conn.topic == convert_utils.TF_STATIC_TOPIC:
                            self.tf_static_def = fixed_def_for_schema
                    
                    if conn.topic not in topic_to_chan_id:
                        meta = {}
                        if conn.topic == convert_utils.TF_STATIC_TOPIC:
                            meta["offered_qos_profiles"] = convert_utils.TF_STATIC_QOS_POLICY
                        
                        cid = writer.register_channel(
                            topic=conn.topic,
                            message_encoding=MessageEncoding.CDR,
                            schema_id=registered_schemas[ros2_type],
                            metadata=meta
                        )
                        topic_to_chan_id[conn.topic] = cid
                        current_stats["topics"][conn.topic] = {"count": 0, "type": ros2_type}

                    conn_map[conn.id] = topic_to_chan_id[conn.topic]
                    type_map[conn.id] = ros2_type

                # --- INJECTION LOGIC ---
                if is_part_of_series and self.static_tf_cache and not self.exiter.stop_requested:
                    chan_id = 0
                    schema_id_to_use = 0
                    if tf_type_name in registered_schemas:
                        schema_id_to_use = registered_schemas[tf_type_name]
                    elif self.tf_static_def:
                        schema_id_to_use = writer.register_schema(name=tf_type_name, encoding=SchemaEncoding.ROS2, data=self.tf_static_def.encode('utf-8'))
                        registered_schemas[tf_type_name] = schema_id_to_use

                    if schema_id_to_use != 0:
                        if convert_utils.TF_STATIC_TOPIC in topic_to_chan_id:
                            chan_id = topic_to_chan_id[convert_utils.TF_STATIC_TOPIC]
                        else:
                            meta = {"offered_qos_profiles": convert_utils.TF_STATIC_QOS_POLICY}
                            chan_id = writer.register_channel(topic=convert_utils.TF_STATIC_TOPIC, message_encoding=MessageEncoding.CDR, schema_id=schema_id_to_use, metadata=meta)
                            topic_to_chan_id[convert_utils.TF_STATIC_TOPIC] = chan_id
                            current_stats["topics"][convert_utils.TF_STATIC_TOPIC] = {"count": 0, "type": tf_type_name}
                    
                    if chan_id != 0:
                        start_time = reader.start_time if reader.start_time else 0
                        from rosbags.typesys.stores.ros2_humble import builtin_interfaces__msg__Time as Time
                        for ros2_msg in self.static_tf_cache:
                            if hasattr(ros2_msg, 'transforms'):
                                for t in ros2_msg.transforms:
                                    t.header.stamp = Time(sec=int(start_time/1e9), nanosec=int(start_time%1e9))
                            cdr_bytes = self.store_ros2.serialize_cdr(ros2_msg, tf_type_name)
                            
                            # FIX 1: Explicit cast to int() for start_time
                            writer.add_message(channel_id=chan_id, log_time=int(start_time), publish_time=int(start_time), data=cdr_bytes)
                            
                            current_stats["topics"][convert_utils.TF_STATIC_TOPIC]["count"] += 1
                            current_stats["message_count"] += 1

                # --- PASS 2: CONVERSION ---
                print("  [INFO] Converting messages...")
                stats_count_total = 0
                
                for conn, timestamp, raw_data in reader.messages():
                    if self.exiter.stop_requested: break
                    if conn.id not in conn_map: continue
                    
                    if writer.just_rotated:
                        if current_stats["start_time"] is None: current_stats["start_time"] = 0
                        if current_stats["end_time"] is None: current_stats["end_time"] = 0
                        current_stats["duration"] = current_stats["end_time"] - current_stats["start_time"]
                        all_file_stats.append(current_stats)
                        current_stats = { "filename": writer.current_file_path.name, "start_time": timestamp, "end_time": timestamp, "message_count": 0, "topics": {} }
                        writer.just_rotated = False

                    ros2_t = type_map[conn.id]
                    try:
                        ros1_msg = self.store_ros1.deserialize_ros1(raw_data, conn.msgtype)
                        ros2_msg = convert_utils.convert_ros1_to_ros2(ros1_msg, ros2_t, self.store_ros2)
                        cdr_bytes = self.store_ros2.serialize_cdr(ros2_msg, ros2_t)

                        # FIX 2: Explicit cast to int() for timestamp
                        writer.add_message(
                            channel_id=conn_map[conn.id],
                            log_time=int(timestamp),
                            publish_time=int(timestamp),
                            data=cdr_bytes
                        )

                        if conn.topic == convert_utils.TF_STATIC_TOPIC:
                            self.static_tf_cache.append(ros2_msg)
                        
                        t_name = conn.topic
                        if t_name not in current_stats["topics"]:
                             current_stats["topics"][t_name] = {"count": 0, "type": ros2_t}

                        current_stats["topics"][t_name]["count"] += 1
                        current_stats["message_count"] += 1
                        stats_count_total += 1
                        
                        if current_stats["start_time"] is None or timestamp < current_stats["start_time"]: current_stats["start_time"] = timestamp
                        if current_stats["end_time"] is None or timestamp > current_stats["end_time"]: current_stats["end_time"] = timestamp

                        if stats_count_total % 10000 == 0: print(f"         {stats_count_total}...", end='\r')
                    
                    except Exception as e:
                        print(f"\n[ERR] Conversion failed on topic {conn.topic} ({ros2_t}):")
                        traceback.print_exc()

                print(f"\n  [SUCCESS] {stats_count_total} messages converted.")

            finally:
                writer.finish()
                if current_stats["message_count"] > 0:
                    if current_stats["start_time"] is None: current_stats["start_time"] = 0
                    if current_stats["end_time"] is None: current_stats["end_time"] = 0
                    current_stats["duration"] = current_stats["end_time"] - current_stats["start_time"]
                    all_file_stats.append(current_stats)

        return all_file_stats

def main():
    parser = argparse.ArgumentParser(description="Convert ROS1 bags to MCAP.")
    parser.add_argument("inputs", nargs='+', help="Input .bag files or a folder")
    parser.add_argument("--series", action="store_true", help="Treat inputs as a split sequence")
    parser.add_argument("--distro", default="humble", help="ROS distro for metadata")
    parser.add_argument("--out-dir", type=Path, default=None, help="Force a specific output directory")
    parser.add_argument("--split-size", default=None, help="Split output files by size (e.g., 3G, 500M)")

    args = parser.parse_args()
    
    try:
        split_bytes = mcap_writer_utils.parse_size(args.split_size)
    except ValueError as e:
        print(f"[ERR] {e}")
        sys.exit(1)

    input_paths = [Path(p) for p in args.inputs]
    bag_files = []
    
    if len(input_paths) == 1 and input_paths[0].is_dir():
        root_folder = input_paths[0]
        bag_files = sorted(list(root_folder.glob("*.bag")))
        print(f"[INFO] Detected folder input: {root_folder} ({len(bag_files)} bags)")
    else:
        bag_files = sorted([p for p in input_paths if p.suffix == ".bag"])

    if not bag_files:
        print("[ERR] No .bag files found.")
        sys.exit(1)

    print(f"[INFO] Processing {len(bag_files)} files. Series Mode: {args.series}")
    
    output_dir = args.out_dir
    if output_dir is None:
        first_file = bag_files[0]
        if args.series:
            stem = first_file.stem
            # Simple heuristic for folder output naming
            if len(input_paths) == 1 and input_paths[0].is_dir():
                parent_name = first_file.parent.name
                output_dir = first_file.parent.parent / f"{parent_name}_mcap"
            else:
                output_dir = first_file.parent / f"{stem}_series_mcap"
            print(f"[INFO] Output Directory: {output_dir}")
        else:
            output_dir = None
    
    if output_dir:
        output_dir.mkdir(parents=True, exist_ok=True)
    
    converter = BagSeriesConverter(ros_distro=args.distro)
    valid_stats = []

    for f in bag_files:
        if converter.exiter.stop_requested: break

        if output_dir:
            dst = output_dir / f.with_suffix(".mcap").name
        else:
            dst = f.with_suffix(".mcap")

        stats_list = converter.convert_file(
            f, dst, 
            split_size=split_bytes, 
            is_part_of_series=args.series
        )
        if stats_list:
            valid_stats.extend(stats_list)
        
        if not args.series:
            converter.static_tf_cache.clear()
            converter.tf_static_def = None

    if output_dir and valid_stats:
        write_metadata_yaml(output_dir, valid_stats, args.distro)

if __name__ == "__main__":
    main()