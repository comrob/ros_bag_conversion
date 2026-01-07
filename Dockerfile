FROM osrf/ros:noetic-desktop-full

# 1. Install basics AND sudo (sudo is required now since we won't be root)
RUN apt-get update && apt-get install -y \
    python3-catkin-tools \
    git \
    nano \
    vim \
    bash-completion \
    sudo \
    python3-pip

RUN pip3 install rosbags mcap-ros2-support zstandard

# 2. Create the user 'dev' with UID 1000
# This ensures files created here are owned by YOU on the host.
ARG USERNAME=dev
ARG USER_UID=1000
ARG USER_GID=1000

RUN groupadd --gid $USER_GID $USERNAME \
    && useradd --uid $USER_UID --gid $USER_GID -m $USERNAME \
    # Give this user password-less sudo access so you can still 'apt install'
    && echo $USERNAME ALL=\(root\) NOPASSWD:ALL > /etc/sudoers.d/$USERNAME \
    && chmod 0440 /etc/sudoers.d/$USERNAME

# 3. Source ROS automatically for this specific user
RUN echo "source /opt/ros/noetic/setup.bash" >> /home/$USERNAME/.bashrc
RUN echo "source /home/$USERNAME/catkin_ws/devel/setup.bash" >> /home/$USERNAME/.bashrc
# 4. Switch context to this user
USER $USERNAME
WORKDIR /home/$USERNAME/catkin_ws