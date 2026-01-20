# ROS1 -> ROS2 (mcap) Bag Conversion

![ROS Logo](https://github.com/comrob/github-media/blob/main/img/image.png?raw=true)

## Quick Start (Demo)

We provide a demo script that **automatically downloads test bagfiles** and runs the conversion logic for you. + The script opens converted file in [foxglove](https://foxglove.dev/download) if it is installed.

```bash
# Run the demo (downloads data -> converts -> verifies)
./run_demo.sh
```

*Prefer to test manually? You can download the sample split-bag dataset from [Google Drive here](https://drive.google.com/drive/folders/18X8cS0u-40FkafMt82_sUuik2VwluMv8?usp=drive_link).*

---

## TL;DR
```
bash ./install.sh

convert_bag /path/to/ros1_bag_folder --series
convert_bag /path/to/single_ros1.bag
convert_bag /path/to/split_0.bag /path/to/split_1.bag --series
```


This repository provides a containerized solution for two primary tasks:

1. **ROSbags Conversion:** A utility to convert ROS 1 (`.bag`) files to ROS 2 (`.mcap`) format. I enjoy a lot the following features:
* **Automatic Type Repair:** Migrates custom messages without source code. Automatically fixes ROS1 vs ROS2 incompatibilities (e.g., stripping `seq` from Headers, fixing `CameraInfo` capitalization).
* **Split-Bag Handling:** Automatically detects split bags and injects `/tf_static` into every chunk. Every output file is self-contained and playable. (e.g., Foxglove or RViz will see the whole TF tree). Generates metadata for seamless playback in ROS 2.
* **Crash-Safe:** Implements graceful signal handling. Hit `Ctrl+C` anytime, and your MCAP file will still be valid and playable. Useful for testing conversion on small data samples.
* **Zero Dependencies:** Dockerized solution. No need to install ROS 1, source workspaces, or build custom message packages just to convert data.


2. **ROS Noetic Development:** A persistent environment for developing, building, and executing ROS 1 (Noetic) packages.

## Prerequisites

* **Docker:** Ensure Docker and Docker Compose are installed.
* **Permissions:** The current user must have permission to run Docker commands (e.g., be in the `docker` group).

---

## Part 1: Bag Converter

The converter runs as an ephemeral container. It mounts the target data directory, processes the files, and terminates. It does not require a local ROS installation.

### Installation

Run the provided installation script to create a symbolic link for the execution wrapper and pull a docker image.

```bash
./install.sh
```

**Note:** Ensure `~/.local/bin` is in your system `$PATH`. You may need to restart your terminal or source your profile after running the script.

### Usage

The `convert_bag` command can be executed from any location. It automatically detects input types and handles Docker volume mounting.

#### 1. Single File Conversion

Converts a single `.bag` file. The output `.mcap` file is created in the same directory as the input.

```bash
convert_bag /path/to/recording.bag

```

#### 2. Series Conversion (Split Bags)

Use the `--series` flag when processing split bag files (e.g., `_0.bag`, `_1.bag`). This mode enables:

* **Static TF Injection:** Collects `/tf_static` from all files and injects them into every subsequent output file to ensure visualization tools work correctly.
* **Metadata Generation:** Generates a `metadata.yaml` file linking the split files for seamless playback in ROS 2.
* **Folder Organization:** Creates a dedicated output directory (e.g., `recording_mcap/`).

**Option A: Input is a Folder**
Processes all `.bag` files within the target directory.

```bash
convert_bag /path/to/data_folder --series

```

*Output Location:* A sibling folder named `data_folder_mcap`.

**Option B: Input is a List of Files**
Processes only the specified files. All files must reside in the same directory.

```bash
convert_bag /path/to/data/bag_0.bag /path/to/data/bag_1.bag --series

```

*Output Location:* A subfolder named `bag_series_mcap` inside the source directory.

### Advanced Configuration

The script accepts optional arguments passed directly to the internal Python converter:

* `--out-dir <path>`: Forces a specific output directory.

<details>
<summary><strong>Feature: Auto-Generating ROS 2 Message Definitions</strong> (Click to Expand)</summary>

The conversion tool embeds message definitions directly into the `.mcap` file. Modern visualization tools like **Foxglove Studio** read these embedded schemas automatically, meaning you can visualize your custom data immediately without any extra steps.

However, if you want to replay the bag using CLI tools (`ros2 bag play`) or inspect topics (`ros2 topic echo`), your local ROS 2 environment needs the compiled message packages installed.

This repository includes a helper script, `additional/extract_mcap_msgs.py`, to automate this process.

**Why use this?**

* **Context:** Run this in your **target ROS 2 environment** (e.g., your robot or a Humble container), not the converter container.
* **Function:** It scans an `.mcap` file, detects all message types that are *not* currently installed in your environment, and auto-generates the complete ROS 2 package source code (CMakeLists.txt, package.xml, .msg files) needed to build them.

**Dependencies:**

Install mcap for python in your ROS 2 environment:

```bash
pip3 install mcap
# if you use Ubunutu 24.04, you might need to force the global installation
pip3 install --break-system-packages mcap
```

**Usage:**

1. **Run the extractor:**
```bash
# Run in your ROS 2 environment
python3 extract_msgs.py /path/to/my_data.mcap --out-dir src/

```


2. **Build the packages:**
```bash
colcon build
source install/setup.bash

```
3. **Check the packages:**
```bash
ros2 interface list | grep <YourCustomMessage>
```


Now `ros2 topic echo` will correctly deserialize your custom messages!

</details>

---

<details>
<summary><strong>Part 2: ROS 1 Development Environment</strong> (Click to Expand)</summary>

The `ros_dev` service provides a full ROS Noetic desktop environment with GUI support.

### Configuration

Map your host directories to the container by editing `docker-compose.yml`:

| Host Path | Container Path | Description |
| --- | --- | --- |
| `~/repos` | `/home/dev/repos` | Source code repositories. |
| `./catkin_ws` | `/home/dev/catkin_ws` | The active Catkin workspace. |
| `/tmp/.X11-unix` | `/tmp/.X11-unix` | X11 socket for GUI applications (Rviz, Rqt). |

### Workflow

1. **Start the Service:**

```bash
docker compose up -d ros_dev

```

2. **Access the Shell:**

```bash
docker exec -it ros_pet_container bash

```

3. **Build and Run:**
Inside the container, the user is `dev`. The environment is pre-configured.

```bash
cd ~/catkin_ws
catkin build
source devel/setup.bash
roslaunch my_package my_node.launch

```

4. **GUI Visualization:**
If your host supports X11 forwarding, you can run GUI tools directly:

```bash
rviz

```

</details>

---

## Troubleshooting

**Error: `convert_bag: command not found**`

* Verify that `~/.local/bin` exists and is included in your `$PATH`.
* Run `export PATH=$PATH:$HOME/.local/bin` to fix it temporarily.

**Error: `docker: command not found**`

* The script invokes Docker. Ensure your user has permissions: `sudo usermod -aG docker $USER`.

**Changes to `convert.py` do not appear**

* **Hot-Reloading:** The current configuration mounts `convert.py` into the container at runtime. Changes to the script on the host are applied immediately without rebuilding.
* **Rebuilding:** If you modify system dependencies (pip packages), you must run `docker compose build converter`.

---


<details>
<summary><strong>Improvement Proposals</strong> (Click to Expand)</summary>

## TODO: Future Proposals

This section outlines planned improvements to enhance usability and maintainability.

### 1. Improve Plug-and-Play Experience

* **Automated Image Publishing:** Publish the Docker image to a registry (e.g., GitHub Container Registry). This would remove the need for users to run `docker compose build`. The script would simply pull the latest version automatically.
* **Self-Bootstrapping Script:** Modify `convert_bag.sh` to check for the Docker image and build it silently if missing, removing the manual installation step entirely.

### 2. Enhance Clarity and Feedback

* **Progress Bars:** Replace the current line-based logging with a library like `tqdm` for visual progress bars during long conversions.
* **Pre-Flight Checks:** Implement a "Dry Run" mode (`--dry-run`) that validates input paths, permissions, and available disk space before starting the conversion.
* **Summary Report:** At the end of execution, print a tabulated summary of converted topics, message counts, and total duration to give the user immediate confidence in the result.

### 3. Architecture

* **Decoupling:** Split the project into two distinct repositories: one for the minimal "Converter Utility" and one for the heavy "Development Environment" to reduce confusion for users who only need one tool.

</details>

---

## Maintainer Guide

To update the system dependencies (Dockerfile):

1.  **Login:** `echo $GITHUB_TOKEN | docker login ghcr.io -u USER --password-stdin`
2.  **Publish:** `./publish_image.sh`