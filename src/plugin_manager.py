import sys
import importlib
import pkgutil
import yaml
from pathlib import Path
from typing import Any, List, Dict, Tuple, Set, Optional

class BasePlugin:
    """Interface that all plugins must inherit from."""
    
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or {}

    def process(self, topic: str, msg: Any, msg_type: str, timestamp: int) -> Tuple[List[Tuple[str, Any, str, Optional[str]]], bool]: 
        """
        Process a message and return a list of emissions + a modification flag.
        
        Args:
            topic: The topic name.
            msg: The ROS message object.
            msg_type: The ROS message type string.
            timestamp: The timestamp in nanoseconds.
            
        Returns:
            A tuple: (emissions_list, was_modified)
            - emissions_list: List of (topic, message, type_string, definition_override) tuples.
              * definition_override: String definition for custom types, or None for standard types.
            - was_modified: Boolean. Set True if ANY change happened.
        """
        # Default: Return original message, None definition, False (not modified)
        return [(topic, msg, msg_type, None)], False

class PluginManager:
    def __init__(self, plugin_dir_path: Path, config_path: Path = None):
        self.active_plugins: List[BasePlugin] = []
        self.available_classes: Dict[str, type] = {}
        self.reported_changes: Set[str] = set()
        
        self._discover_plugins(plugin_dir_path)

        if config_path and config_path.exists():
            self._load_from_config(config_path)
        else:
            print("[PLUGIN] No config file found. No plugins active.")

    def _discover_plugins(self, plugin_dir: Path):
        if not plugin_dir.exists(): return
        sys.path.append(str(plugin_dir.resolve()))
        
        for _, name, _ in pkgutil.iter_modules([str(plugin_dir)]):
            try:
                module = importlib.import_module(name)
                for attr_name in dir(module):
                    attr = getattr(module, attr_name)
                    if isinstance(attr, type) and issubclass(attr, BasePlugin) and attr is not BasePlugin:
                        self.available_classes[attr_name] = attr
            except Exception:
                pass

    def _load_from_config(self, config_path: Path):
        try:
            with open(config_path, 'r') as f:
                full_config = yaml.safe_load(f)
            
            plugins_conf = full_config.get('plugins', {})
            
            if not plugins_conf:
                print("[PLUGIN] Config file is empty or missing 'plugins' key.")
                return

            print(f"[PLUGIN] Config loaded. Processing {len(plugins_conf)} entries...")

            for class_name, params in plugins_conf.items():
                if params is None: 
                    # Allow disabling by setting to null in YAML
                    continue
                
                if class_name in self.available_classes:
                    print(f"  -> Activating '{class_name}'")
                    # Instantiate with specific config
                    plugin_instance = self.available_classes[class_name](config=params)
                    self.active_plugins.append(plugin_instance)
                else:
                    print(f"[PLUGIN] [WARN] Class '{class_name}' not found in source files.")

        except Exception as e:
            print(f"[PLUGIN] [ERR] Error reading config: {e}")

    def run_plugins(self, topic: str, msg: Any, msg_type: str, timestamp: int) -> List[Tuple[str, Any, str, Optional[str]]]:
        # Initialize pipeline with the input message
        current_emissions = [(topic, msg, msg_type, None)]
        
        for plugin in self.active_plugins:
            if not current_emissions: break
            
            next_step_emissions = []
            
            # The output of the previous plugin is the input for the current one
            for (t, m, mt, _) in current_emissions:
                try:
                    # Execute Plugin
                    results, modified = plugin.process(t, m, mt, timestamp)
                    
                    if results:
                        next_step_emissions.extend(results)
                        
                        # Logging based on the EXPLICIT flag
                        if modified:
                            report_key = f"{plugin.__class__.__name__}::{t}"
                            if report_key not in self.reported_changes:
                                print(f"[PLUGIN REPORT] {plugin.__class__.__name__} >> MODIFIED {t}")
                                self.reported_changes.add(report_key)
                    
                except Exception as e:
                    print(f"[PLUGIN ERR] {plugin.__class__.__name__}: {e}")
                    # On crash, preserve input to ensure no data loss
                    next_step_emissions.append((t, m, mt, None))
            
            current_emissions = next_step_emissions
        
        return current_emissions