# ROS Bag Conversion & Development Toolkit

This repository provides a streamlined Docker environment for two distinct purposes:
1.  **Converter:** A standalone utility to convert ROS 1 (`.bag`) files to ROS 2 (`.mcap`) with support for split bags and static TF injection.
2.  **Dev Environment:** A persistent container for developing, building, and running ROS 1 (Noetic) packages.

---

## ⚡ Quick Start: The Converter

No ROS installation is required on your host. Just Docker.

### 1. Installation
Run the install script to build the Docker image and link the command to your path.

```bash
# Builds the image 'ros_bag_converter' and symlinks the script to ~/.local/bin
docker compose build converter
./install.sh

```

*(Ensure `~/.local/bin` is in your `$PATH`. You may need to restart your terminal).*

### 2. Usage

You can now run `convert_bag` from any directory on your computer.

**Single File Conversion**

```bash
convert_bag /path/to/my_recording.bag

```

*Output: `/path/to/my_recording.mcap*`

**Split Bags (Series) Conversion**
If you have a folder of split bags (e.g., `_0.bag`, `_1.bag`), point to the folder and add `--series`:

```bash
convert_bag /path/to/recording_folder --series

```

**Advanced Options**
The script passes flags directly to the internal Python converter:

```bash

# Convert specific files in the current folder
convert_bag bag1.bag bag2.bag --series

```

---

## 🛠️ ROS 1 Development Environment

The `ros_dev` service is a full desktop environment for compiling and running ROS 1 nodes.

### 📂 Directory Setup

Ensure your host directories map to the container as defined in `docker-compose.yml`:

| Host Path | Container Path | Purpose |
| --- | --- | --- |
| `~/repos` | `/home/dev/repos` | Your source code/git repositories. |
| `./catkin_ws` | `/home/dev/catkin_ws` | Active ROS workspace. |
| `/tmp/.X11-unix` | `/tmp/.X11-unix` | For GUI/Rviz support. |

### Workflow

**1. Start the Container**

```bash
docker compose up -d ros_dev

```

**2. Enter the Shell**

```bash
docker exec -it ros_pet_container bash

```

**3. Build & Run**
Inside the container, you are the user `dev`.

```bash
# Build workspace
catkin build

# Source environment
source devel/setup.bash

# Run nodes
roslaunch my_package my.launch

```

**4. GUI Apps (Rviz/Rqt)**
The container shares the host's X11 socket.

```bash
rviz

```

---

## ⚠️ Troubleshooting

**`convert_bag: command not found`**

* Ensure `~/.local/bin` is in your PATH. Add `export PATH=$PATH:$HOME/.local/bin` to your `~/.bashrc`.

**`docker: command not found` inside the script**

* You might need to add your user to the docker group: `sudo usermod -aG docker $USER`.

**Updates to `convert.py` are ignored**

* The Python script is baked into the Docker image. If you edit `convert.py`, you **must** rebuild:
```bash
docker compose build converter

```
