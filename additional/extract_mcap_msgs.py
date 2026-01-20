import argparse
import sys
import os
import re
from pathlib import Path
from typing import Dict, Set, List, Tuple

# MCAP reading
from mcap.reader import make_reader
from mcap.records import Schema

# ROS 2 Environment Inspection
try:
    from ament_index_python.packages import get_package_share_directory, get_packages_with_prefixes
    ROS2_ENV_LOADED = True
except ImportError:
    ROS2_ENV_LOADED = False

# --- templates ---

PACKAGE_XML_TEMPLATE = """<?xml version="1.0"?>
<?xml-model href="http://download.ros.org/schema/package_format3.xsd" schematypens="http://www.w3.org/2001/XMLSchema"?>
<package format="3">
  <name>{pkg_name}</name>
  <version>0.0.1</version>
  <description>Auto-generated from MCAP file</description>
  <maintainer email="user@example.com">MCAP Extractor</maintainer>
  <license>TODO</license>

  <buildtool_depend>ament_cmake</buildtool_depend>
  <buildtool_depend>rosidl_default_generators</buildtool_depend>

{dependencies}

  <member_of_group>rosidl_interface_packages</member_of_group>

  <export>
    <build_type>ament_cmake</build_type>
  </export>
</package>
"""

CMAKE_TEMPLATE = """cmake_minimum_required(VERSION 3.8)
project({pkg_name})

if(CMAKE_COMPILER_IS_GNUCXX OR CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  add_compile_options(-Wall -Wextra -Wpedantic)
endif()

find_package(ament_cmake REQUIRED)
find_package(rosidl_default_generators REQUIRED)

{find_packages}

rosidl_generate_interfaces(${{PROJECT_NAME}}
  {msg_files}
  DEPENDENCIES
  {dep_list}
)

ament_export_dependencies(rosidl_default_runtime)

ament_package()
"""

# --- Helpers ---

def get_installed_packages() -> Set[str]:
    """Returns a set of all ROS 2 packages currently installed/sourced."""
    if not ROS2_ENV_LOADED:
        print("[WARN] 'ament_index_python' not found. Assuming clean environment (generating ALL packages).")
        return set()
    return set(get_packages_with_prefixes().keys())

def clean_msg_def(text: str) -> str:
    """Cleans up the raw schema text for .msg file generation."""
    lines = []
    for line in text.splitlines():
        line = line.strip()
        
        # 1. Skip separators (handled by splitter) and empty lines
        if line.startswith("=") or line.startswith("MSG:") or not line:
            continue
        
        # 2. Strip comments
        line = line.split('#')[0].rstrip()
        if not line: continue

        # 3. FIX: Ensure types are "std_msgs/Header", NOT "std_msgs/msg/Header"
        line = re.sub(r'([a-zA-Z0-9_]+)/msg/([a-zA-Z0-9_]+)', r'\1/\2', line)
            
        lines.append(line)
        
    return "\n".join(lines).strip()

def parse_dependencies(msg_text: str) -> Set[str]:
    """Scans a .msg content to find dependencies."""
    deps = set()
    # Matches "pkg/Type" or "Type" (though "Type" is hard to distinguish from field names without context)
    # We focus on explicit "pkg/Type"
    matches = re.findall(r'([a-zA-Z0-9_]+)/(?:msg/)?([a-zA-Z0-9_]+)', msg_text)
    
    for pkg, _ in matches:
        if not pkg.islower(): continue # Ignore CamelCase class names
        deps.add(pkg)
    return deps

def extract_sub_messages(raw_text: str, root_pkg: str, root_msg: str) -> List[Tuple[str, str, str]]:
    """
    Splits a ROS 1 recursive definition into chunks.
    Returns list of (pkg_name, msg_name, msg_text).
    """
    # Regex to split by the separator line "================..."
    chunks = re.split(r'^=+\n+', raw_text, flags=re.MULTILINE)
    
    results = []
    
    # Chunk 0 is ALWAYS the root message
    results.append((root_pkg, root_msg, chunks[0]))
    
    # Subsequent chunks start with "MSG: package/Type"
    for chunk in chunks[1:]:
        chunk = chunk.strip()
        if not chunk: continue
        
        lines = chunk.splitlines()
        header = lines[0].strip() # e.g. "MSG: septentrio_gnss_driver/BlockHeader"
        
        if header.startswith("MSG:"):
            # Parse package and name
            type_str = header.split("MSG:")[1].strip()
            parts = type_str.split("/")
            
            pkg = None
            name = None
            
            # SAFE HANDLING of split results:
            if len(parts) == 2:
                # Case: "package/Type"
                pkg, name = parts
            elif len(parts) == 3:
                # Case: "package/msg/Type"
                pkg, _, name = parts
            
            if pkg and name:
                content = "\n".join(lines[1:]) # The rest is the body
                results.append((pkg, name, content))
                
    return results

# --- Main Logic ---

def main():
    parser = argparse.ArgumentParser(description="Extract missing ROS 2 message definitions from MCAP.")
    parser.add_argument("input", type=Path, help="Input .mcap file")
    parser.add_argument("--out-dir", type=Path, default=Path("generated_msgs"), help="Output source directory")
    args = parser.parse_args()

    if not args.input.exists():
        print(f"[ERR] File not found: {args.input}")
        sys.exit(1)

    installed_pkgs = get_installed_packages()
    print(f"[INFO] Detected {len(installed_pkgs)} installed ROS 2 packages.")

    # Storage for generation: {pkg_name: {msg_name: def_text}}
    schemas_to_gen: Dict[str, Dict[str, str]] = {} 
    
    print(f"[INFO] Scanning {args.input.name}...")
    with open(args.input, "rb") as f:
        reader = make_reader(f)
        summary = reader.get_summary()
        
        if not summary or not summary.schemas:
            print("[ERR] No schemas found.")
            sys.exit(1)

        for schema in summary.schemas.values():
            if schema.encoding != "ros2msg": pass

            # 1. Parse Root Name
            parts = schema.name.split('/')
            if len(parts) == 3 and parts[1] == 'msg':
                root_pkg, _, root_msg = parts
            elif len(parts) == 2:
                root_pkg, root_msg = parts
            else:
                continue 
            
            # 2. Extract Deep Definitions (Recursively)
            raw_data = schema.data.decode('utf-8', errors='ignore')
            all_msgs = extract_sub_messages(raw_data, root_pkg, root_msg)
            
            # 3. Register for generation (if not installed)
            for pkg, msg, text in all_msgs:
                if pkg in installed_pkgs:
                    continue # Skip std_msgs, geometry_msgs, etc.
                
                if pkg not in schemas_to_gen:
                    schemas_to_gen[pkg] = {}
                
                # Only save if not already found (avoid overwriting with duplicate recursive blocks)
                if msg not in schemas_to_gen[pkg]:
                    schemas_to_gen[pkg][msg] = text

    if not schemas_to_gen:
        print("[INFO] No missing custom packages found.")
        sys.exit(0)

    print(f"[INFO] Found {len(schemas_to_gen)} custom packages to generate.")

    # 4. Generate Code
    for pkg, msgs in schemas_to_gen.items():
        pkg_dir = args.out_dir / pkg
        msg_dir = pkg_dir / "msg"
        msg_dir.mkdir(parents=True, exist_ok=True)
        
        all_pkg_deps = set()
        msg_files_list = []

        print(f"  -> Generating {pkg} ({len(msgs)} msgs)...")

        # Write .msg files
        for msg_name, msg_def in msgs.items():
            clean_def = clean_msg_def(msg_def)
            
            # Track dependencies
            deps = parse_dependencies(clean_def)
            for d in deps:
                if d != pkg: all_pkg_deps.add(d)

            fname = f"{msg_name}.msg"
            with open(msg_dir / fname, "w") as f_msg:
                f_msg.write(clean_def)
            
            msg_files_list.append(f"msg/{fname}")

        # Create package.xml
        dep_xml_lines = [f"  <depend>{d}</depend>" for d in sorted(all_pkg_deps)]
        
        with open(pkg_dir / "package.xml", "w") as f_xml:
            f_xml.write(PACKAGE_XML_TEMPLATE.format(
                pkg_name=pkg,
                dependencies="\n".join(dep_xml_lines)
            ))

        # Create CMakeLists.txt
        find_pkg_lines = [f"find_package({d} REQUIRED)" for d in sorted(all_pkg_deps)]
        dep_list_cmake = sorted(all_pkg_deps)

        with open(pkg_dir / "CMakeLists.txt", "w") as f_cmake:
            f_cmake.write(CMAKE_TEMPLATE.format(
                pkg_name=pkg,
                find_packages="\n".join(find_pkg_lines),
                msg_files="\n  ".join(msg_files_list),
                dep_list="\n  ".join(dep_list_cmake)
            ))

    print("\n[SUCCESS] Generation complete.")
    print(f"1. Delete old failed folders: 'rm -rf src/compass_msgs src/gps_common src/septentrio_gnss_driver'")
    print(f"2. Copy '{args.out_dir}/*' to 'src/'")
    print("3. Run 'colcon build'")

if __name__ == "__main__":
    main()