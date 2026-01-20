import sys
import re
from pathlib import Path
import numpy as np

# ROS1 Reader
from rosbags.rosbag1 import Reader
# Type System
from rosbags.typesys import get_types_from_msg, get_typestore, Stores

# MCAP Writer
from mcap.writer import Writer as McapWriter
from mcap.well_known import SchemaEncoding, MessageEncoding

# --- Configuration ---
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

def get_qos_profile(topic: str) -> str:
    if topic == '/tf_static':
        return TF_STATIC_QOS_POLICY
    return ""

def to_ros2_type(ros1_type: str) -> str:
    """Converts 'pkg/Type' -> 'pkg/msg/Type'"""
    # Explicit mapping for known problematic types
    if ros1_type == 'Header': return 'std_msgs/msg/Header'
    
    if "/msg/" not in ros1_type and "/" in ros1_type:
        parts = ros1_type.split('/')
        if len(parts) == 2:
            return f"{parts[0]}/msg/{parts[1]}"
    return ros1_type

def fix_ros1_def(text: str, type_name: str) -> str:
    """
    Parses ROS1 definitions line-by-line to safely convert types to ROS2.
    """
    lines = text.splitlines()
    fixed_lines = []
    
    for line in lines:
        stripped = line.strip()
        if not stripped:
            fixed_lines.append(line)
            continue
            
        # 1. REMOVE 'seq'
        if stripped.startswith("uint32 seq"):
            continue

        # 2. Handle Separator Lines (MSG: ...)
        if stripped.startswith("MSG:"):
            # MSG: package/Type -> MSG: package/msg/Type
            line = re.sub(r'(^MSG:\s+)([a-zA-Z0-9_]+)/(?!msg/)([a-zA-Z0-9_]+)', r'\1\2/msg/\3', line)
            fixed_lines.append(line)
            continue

        # 3. Handle Field Definitions
        # Regex captures: (Indent)(Type)(Whitespace)(Name)(Rest)
        match = re.match(r'^(\s*)([a-zA-Z0-9_/\[\]]+)(\s+)([^#\s]+)(.*)$', line)
        
        if match:
            indent, type_str, ws, name_str, rest = match.groups()
            
            # --- A. FIX THE TYPE ---
            if type_str == 'time': type_str = 'builtin_interfaces/msg/Time'
            elif type_str == 'duration': type_str = 'builtin_interfaces/msg/Duration'
            elif type_str == 'Header': type_str = 'std_msgs/msg/Header'
            
            # Inject /msg/ into ROS types (pkg/Type -> pkg/msg/Type)
            if '/' in type_str and '/msg/' not in type_str:
                base_type = type_str.split('[')[0] 
                suffix = type_str[len(base_type):] 
                parts = base_type.split('/')
                if len(parts) == 2:
                    type_str = f"{parts[0]}/msg/{parts[1]}{suffix}"
            
            # Fix intra-package references (e.g., GPSStatus inside GPSFix)
            # If we see a type like "GPSStatus" without a package, we assume it's local.
            # But we must make it relative or fully qualified for ROS2 parser if needed.
            # For robustness, we leave it alone if it has no slash; rosbags handles relative types well
            # IF the sub-definition header is correct.

            # --- B. FIX THE NAME (CameraInfo only) ---
            if "CameraInfo" in type_name:
                if name_str == 'D': name_str = 'd'
                elif name_str == 'K': name_str = 'k'
                elif name_str == 'R': name_str = 'r'
                elif name_str == 'P': name_str = 'p'

            new_line = f"{indent}{type_str}{ws}{name_str}{rest}"
            fixed_lines.append(new_line)
        else:
            fixed_lines.append(line)

    return "\n".join(fixed_lines)

def convert_ros1_to_ros2(ros1_obj, ros2_type, ros2_store):
    try:
        MsgType = ros2_store.types[ros2_type]
    except KeyError:
        # Fallback: if mapping failed, maybe try the ROS1 name? 
        # But usually this means registration failed.
        return ros1_obj

    kwargs = {}
    for field_name, field_desc in MsgType.__dataclass_fields__.items():
        # --- 1. FIELD MAPPING ---
        val = getattr(ros1_obj, field_name, None)
        if val is None:
            val = getattr(ros1_obj, field_name.upper(), None)

        # --- 2. Handle Missing / Matrices ---
        if val is None:
            if field_name in ['d', 'k', 'r', 'p', 'binning_x', 'binning_y', 'covariance']:
                size = 0
                if field_name in ['k', 'r', 'covariance']: size = 9
                elif field_name == 'p': size = 12
                val = np.zeros(size, dtype=np.float64)
            else:
                continue

        # --- 3. Handle Numeric Arrays ---
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

        # --- 4. Recursion ---
        if hasattr(val, '__msgtype__'):
            sub_type_ros1 = val.__msgtype__
            sub_type_ros2 = to_ros2_type(sub_type_ros1)
            val = convert_ros1_to_ros2(val, sub_type_ros2, ros2_store)
        elif isinstance(val, list):
             if len(val) > 0 and hasattr(val[0], '__msgtype__'):
                 sub_type_ros1 = val[0].__msgtype__
                 sub_type_ros2 = to_ros2_type(sub_type_ros1)
                 val = [convert_ros1_to_ros2(x, sub_type_ros2, ros2_store) for x in val]
        
        # --- 5. Time/Duration ---
        if hasattr(val, 'secs') and hasattr(val, 'nsecs'):
             if 'builtin_interfaces/msg/Time' in str(field_desc.type):
                 from rosbags.typesys.stores.ros2_humble import builtin_interfaces__msg__Time as Time
                 val = Time(sec=val.secs, nanosec=val.nsecs)
             elif 'builtin_interfaces/msg/Duration' in str(field_desc.type):
                 from rosbags.typesys.stores.ros2_humble import builtin_interfaces__msg__Duration as Duration
                 val = Duration(sec=val.secs, nanosec=val.nsecs)

        kwargs[field_name] = val
    return MsgType(**kwargs)

def convert_bag(src: Path, dst: Path):
    print(f"[INFO] Initializing Stores...")
    store_ros1 = get_typestore(Stores.ROS1_NOETIC)
    store_ros2 = get_typestore(Stores.ROS2_HUMBLE)

    print(f"[INFO] Opening source: {src}")
    with Reader(src) as reader:
        print(f"[INFO] Opening destination: {dst}")
        
        with open(dst, "wb") as f_out:
            writer = McapWriter(f_out)
            writer.start()

            # Wrap the entire process in a Try/Except block to catch the first Ctrl+C
            try:
                print("[INFO] Registering schemas and channels...")
                
                topic_to_chan_id = {}
                conn_map = {}
                type_map = {}
                registered_schemas = {} 

                # --- PASS 1: REGISTRATION ---
                for conn in reader.connections:
                    msg_def = None
                    if hasattr(conn, 'msgdef'): msg_def = conn.msgdef
                    elif hasattr(conn, 'ext'):
                        if isinstance(conn.ext, dict): msg_def = conn.ext.get('message_definition', None)
                        elif hasattr(conn.ext, 'message_definition'): msg_def = conn.ext.message_definition
                    
                    if not msg_def and conn.msgtype in reader.msgtypes:
                        msg_def = reader.msgtypes[conn.msgtype]
                    
                    if not msg_def:
                        continue

                    ros1_type = conn.msgtype
                    ros2_type = to_ros2_type(ros1_type)
                    
                    # A. Register in ROS1 Store
                    try:
                        types_v1 = get_types_from_msg(msg_def, ros1_type)
                        store_ros1.register(types_v1)
                    except Exception:
                        pass 

                    # B. Fix and Register in ROS2 Store
                    fixed_def = fix_ros1_def(msg_def, ros2_type)
                    try:
                        types_v2 = get_types_from_msg(fixed_def, ros2_type)
                        store_ros2.register(types_v2)
                    except Exception as e:
                        print(f"[ERR] Failed to register type {ros2_type}: {e}")

                    # C. Register Schema in MCAP
                    if ros2_type not in registered_schemas:
                        try:
                            schema_id = writer.register_schema(
                                name=ros2_type,
                                encoding=SchemaEncoding.ROS2, 
                                data=fixed_def.encode('utf-8')
                            )
                            registered_schemas[ros2_type] = schema_id
                        except Exception as e:
                            print(f"[ERR] MCAP Schema error {ros2_type}: {e}")
                            continue
                    else:
                        schema_id = registered_schemas[ros2_type]

                    # D. Register Channel
                    if conn.topic not in topic_to_chan_id:
                        metadata = {}
                        qos_profile = get_qos_profile(conn.topic)
                        if qos_profile:
                            metadata["offered_qos_profiles"] = qos_profile

                        chan_id = writer.register_channel(
                            topic=conn.topic,
                            message_encoding=MessageEncoding.CDR,
                            schema_id=schema_id,
                            metadata=metadata
                        )
                        topic_to_chan_id[conn.topic] = chan_id
                    
                    conn_map[conn.id] = topic_to_chan_id[conn.topic]
                    type_map[conn.id] = ros2_type

                # --- PASS 2: CONVERSION ---
                print("[INFO] Converting messages... (Press Ctrl+C to stop early)")
                count = 0
                
                for conn, timestamp, raw_data in reader.messages():
                    if conn.id not in conn_map: continue
                    
                    try:
                        ros2_typename = type_map[conn.id]
                        ros1_typename = conn.msgtype
                        chan_id = conn_map[conn.id]

                        ros1_msg = store_ros1.deserialize_ros1(raw_data, ros1_typename)
                        ros2_msg = convert_ros1_to_ros2(ros1_msg, ros2_typename, store_ros2)
                        cdr_bytes = store_ros2.serialize_cdr(ros2_msg, ros2_typename)

                        writer.add_message(
                            channel_id=chan_id,
                            log_time=timestamp,
                            publish_time=timestamp,
                            data=cdr_bytes
                        )
                        
                        count += 1
                        if count % 5000 == 0: print(f"       {count}...", end='\r')

                    except Exception as e:
                        pass
                
                print(f"\n[SUCCESS] Completed. Processed {count} messages.")

            except KeyboardInterrupt:
                # 1st Ctrl+C lands here
                print(f"\n[WARN] Interrupted by user! Finalizing MCAP file safely...")
                print(f"[TIP]  Press Ctrl+C AGAIN to hard-stop immediately (may corrupt file).")
            
            finally:
                # 2nd Ctrl+C (during this step) will crash the script naturally
                writer.finish()
                print(f"[INFO] File finalized: {dst}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python convert_ros1_to_mcap.py <input.bag> <output.mcap>")
        sys.exit(1)
    convert_bag(Path(sys.argv[1]), Path(sys.argv[2]))