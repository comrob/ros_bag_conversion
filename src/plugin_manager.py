import sys
import importlib
import pkgutil
import yaml
from pathlib import Path
from typing import Any, List, Dict, Tuple, Set

class BasePlugin:
    """Interface that all plugins must inherit from."""
    # CHANGED: Added timestamp argument
    def process(self, topic: str, msg: Any, msg_type: str, timestamp: int) -> Tuple[Any, bool]: 
        """
        Must return a tuple: (message, was_modified)
        timestamp: The message timestamp in nanoseconds (from the bag file).
        """
        return msg, False

class PluginManager:
    def __init__(self, plugin_dir_path: Path, config_path: Path = None):
        self.active_plugins: List[BasePlugin] = []
        self.available_classes: Dict[str, type] = {}
        
        # Memory to ensure we only log a change once per topic/plugin pair
        self.reported_changes: Set[str] = set()
        
        self._discover_plugins(plugin_dir_path)

        if config_path and config_path.exists():
            self._load_from_config(config_path)
        else:
            print("[PLUGIN] no plugins applied")

    def _discover_plugins(self, plugin_dir: Path):
        if not plugin_dir.exists(): return
        sys.path.append(str(plugin_dir.resolve()))
        
        for _, name, _ in pkgutil.iter_modules([str(plugin_dir)]):
            try:
                module = importlib.import_module(name)
                for attr_name in dir(module):
                    attribute = getattr(module, attr_name)
                    if isinstance(attribute, type) and \
                       issubclass(attribute, BasePlugin) and \
                       attribute is not BasePlugin:
                        self.available_classes[attr_name] = attribute
            except Exception:
                pass

    def _load_from_config(self, config_path: Path):
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
            
            if not config:
                print("[PLUGIN] no plugins applied")
                return

            plugin_names = config.get('active_plugins', [])
            if not plugin_names:
                print("[PLUGIN] no plugins applied")
                return

            print(f"[PLUGIN] Config loaded. Activating: {plugin_names}")

            for name in plugin_names:
                if name in self.available_classes:
                    self.active_plugins.append(self.available_classes[name]())
                else:
                    print(f"[PLUGIN] [WARN] Plugin '{name}' not found.")
        except Exception as e:
            print(f"[PLUGIN] [ERR] Error reading config: {e}")

    def run_plugins(self, topic: str, msg: Any, msg_type: str, timestamp: int) -> Any:
        current_msg = msg
        for plugin in self.active_plugins:
            if current_msg is None: break 
            try:
                # Execute plugin
                current_msg, modified = plugin.process(topic, current_msg, msg_type, timestamp)
                
                # Vocal Reporting (Deduplicated)
                if modified:
                    # Create a unique key for this specific event
                    report_key = f"{plugin.__class__.__name__}::{topic}"
                    
                    if report_key not in self.reported_changes:
                        print(f"[PLUGIN REPORT] {plugin.__class__.__name__} >> CHANGED {topic}")
                        self.reported_changes.add(report_key)
                    
            except Exception as e:
                print(f"[PLUGIN ERR] {plugin.__class__.__name__}: {e}")
        
        return current_msg