mkdir -p ~/.local/bin
ln -sf ~/docker_ros/convert_bag.sh ~/.local/bin/convert_bag

# Check if ~/.local/bin is in PATH
if [[ ":$PATH:" != *":$HOME/.local/bin:"* ]]; then
    echo "# Added by ros_bag_conversion's install.sh" >> ~/.bashrc
    echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.bashrc
    # Optional: Export for the current session so you can use it immediately without restarting
    export PATH="$HOME/.local/bin:$PATH"
    echo "Added ~/.local/bin to PATH in .bashrc"
fi

# pre-pull docker image so that it does not take time on first run
docker compose -f ~/docker_ros/docker-compose.yml pull converter