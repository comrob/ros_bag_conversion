import cv2
import numpy as np
from typing import Any, Tuple
from plugin_manager import BasePlugin

class DebayerPlugin(BasePlugin):
    def process(self, topic: str, msg: Any, msg_type: str, timestamp: int) -> Tuple[Any, bool]:
        # 1. Filter
        if "camera_front" not in topic or "CompressedImage" not in msg_type:
            return msg, False  # <--- NOT MODIFIED
        
        fmt = getattr(msg, 'format', '').lower()
        if "bayer" not in fmt:
            return msg, False  # <--- NOT MODIFIED

        try:
            np_arr = np.frombuffer(msg.data, np.uint8)
            bayer_img = cv2.imdecode(np_arr, cv2.IMREAD_GRAYSCALE)

            if bayer_img is None:
                return msg, False # <--- FAILED/SKIPPED

            # Debayer (BGGR Fix)
            color_img = cv2.cvtColor(bayer_img, cv2.COLOR_BayerBG2BGR)

            encode_param = [int(cv2.IMWRITE_JPEG_QUALITY), 95]
            success, enc_buf = cv2.imencode(".jpg", color_img, encode_param)

            if success:
                msg.data = enc_buf.flatten()
                msg.format = "jpeg; rgb8" 
                return msg, True # <--- VOCAL FLAG: "I CHANGED THIS"

        except Exception as e:
            print(f"[PLUGIN ERR] Debayer failed on {topic}: {e}")

        return msg, False