import sys
import re
import signal
import numpy as np
from typing import Any, Optional

# Constants
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