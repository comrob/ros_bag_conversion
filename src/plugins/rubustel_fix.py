import sys
import re
import json
from typing import Any, Tuple
from plugin_manager import BasePlugin

class RobustelFixPlugin(BasePlugin):
    # Updated signature to match BasePlugin
    def process(self, topic: str, msg: Any, msg_type: str, timestamp: int) -> Tuple[Any, bool]:
        
        # 1. Filter
        if "robustel" not in topic:
            return msg, False

        try:
            # 2. Extract Raw Text 
            raw_text = ""
            if hasattr(msg, 'data'):
                if isinstance(msg.data, str): raw_text = msg.data
                elif isinstance(msg.data, bytes): raw_text = msg.data.decode('utf-8', errors='ignore')
                else: raw_text = str(msg)
            else:
                raw_text = str(msg)

            # 3. Parse fields
            parsed_data = {}
            patterns = {
                "rsrp_value": r"rsrp_value\s*=\s*([-+]?\d+(?:\.\d+)?)",
                "network_type": r"network_type\s*=\s*(\w+)",
                "cell_id": r"cell_id\s*=\s*(\d+)",
                "band": r"band\s*=\s*(\d+)",
                "sinr_value": r"sinr_value\s*=\s*([-+]?\d+(?:\.\d+)?)", # Added useful signal stats
                "rsrq_value": r"rsrq_value\s*=\s*([-+]?\d+(?:\.\d+)?)"
            }

            found_any = False
            for key, pattern in patterns.items():
                match = re.search(pattern, raw_text)
                if match:
                    val = match.group(1)
                    if "." in val: parsed_data[key] = float(val)
                    elif val.isdigit() or (val.startswith("-") and val[1:].isdigit()): parsed_data[key] = int(val)
                    else: parsed_data[key] = val
                    found_any = True

            if found_any:
                # 4. INJECT TIMESTAMP (The new feature)
                # 'timestamp' is in nanoseconds (int). 
                # We add a float version for easy Python usage later.
                parsed_data['timestamp_ns'] = timestamp
                parsed_data['timestamp'] = timestamp / 1e9 

                # 5. Overwrite
                json_output = json.dumps(parsed_data)
                
                if hasattr(msg, 'data'):
                    msg.data = json_output

                # Remove the debug print/exit once you are confident
                # print(f"[DEBUG] JSON with Time: {json_output}")
                
                return msg, True

        except Exception as e:
            print(f"[PLUGIN ERR] Robustel p failed: {e}")

        return msg, False