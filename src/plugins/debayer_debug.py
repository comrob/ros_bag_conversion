import sys
import cv2
import numpy as np
from typing import Any
from plugin_manager import BasePlugin

class DebayerDebugPlugin(BasePlugin):
    def process(self, topic: str, msg: Any, msg_type: str, timestamp: int) -> Any:
        # 1. Filter: Run only on Front Camera Compressed Images
        if "camera_front" not in topic or "CompressedImage" not in msg_type:
            return msg
        
        fmt = getattr(msg, 'format', '').lower()
        if "bayer" not in fmt:
            return msg

        try:
            # 2. Decompress
            np_arr = np.frombuffer(msg.data, np.uint8)
            bayer_img = cv2.imdecode(np_arr, cv2.IMREAD_GRAYSCALE)

            if bayer_img is None: return msg

            # 3. Debayer (Using the BGGR fix we found)
            color_img = cv2.cvtColor(bayer_img, cv2.COLOR_BayerBG2BGR)

            # 4. Re-compress to JPEG
            encode_param = [int(cv2.IMWRITE_JPEG_QUALITY), 95]
            success, enc_buf = cv2.imencode(".jpg", color_img, encode_param)

            if success:
                # --- FIX IS HERE ---
                # Do NOT use .tobytes(). Keep it as a numpy array.
                # flatten() ensures it is a 1D array (uint8), which rosbags expects.
                msg.data = enc_buf.flatten()
                
                # Update format string
                msg.format = "jpeg; rgb8" 

            # 5. Debug: Save to disk and Exit
            print(f"\n[DEBUG] ROS2 Message updated successfully.")
            print(f"        New Data Size: {len(msg.data)} bytes")
            print(f"        New Format: {msg.format}")

            cv2.imwrite("debug_final_result.jpg", color_img)
            print("  -> Saved 'debug_final_result.jpg' to disk.")
            
            print("[DEBUG] Exiting script now.")
            sys.exit(0)

        except SystemExit:
            raise
        except Exception as e:
            print(f"[PLUGIN ERR] {e}")

        return msg