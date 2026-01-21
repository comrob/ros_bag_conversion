import re
from pathlib import Path
from typing import Optional
from mcap.writer import Writer as McapWriter

def parse_size(size_str: Optional[str]) -> int:
    """Converts human readable size (3G, 500M) to bytes. Returns 0 if None."""
    if not size_str:
        return 0
    units = {"K": 1024, "M": 1024**2, "G": 1024**3, "T": 1024**4}
    match = re.match(r'^(\d+)([KMGT]?)$', size_str.upper())
    if match:
        val, unit = match.groups()
        return int(val) * units.get(unit, 1)
    raise ValueError(f"Invalid size format: {size_str}")

class RollingMcapWriter:
    def __init__(self, base_path: Path, split_size: int, profile: str):
        self.base_path = base_path
        self.split_size = split_size
        self.profile = profile
        self.part_num = 0
        
        # State preservation for rotation
        self.schemas = {}
        self.channels = {}
        
        # Current active writer state
        self.writer = None
        self._f = None
        self.current_file_path = None
        
        # Signal that a rotation happened
        self.just_rotated = False

        self._open_new_segment()
        
        # FIX: Reset this flag. The initial file open is not a "rotation" 
        # that needs to trigger a stats save in the main loop.
        self.just_rotated = False

    def _open_new_segment(self):
        """Closes current and opens next file segment."""
        if self.writer:
            self.writer.finish()
            self._f.close()

        # --- NAMING FIX START ---
        # Part 0: Use the exact filename provided (e.g., test_0.mcap)
        # Part 1+: Append index (e.g., test_0_1.mcap)
        if self.part_num == 0:
            self.current_file_path = self.base_path
        else:
            stem = self.base_path.stem
            new_name = f"{stem}_{self.part_num}.mcap"
            self.current_file_path = self.base_path.parent / new_name
        # --- NAMING FIX END ---
        
        self._f = open(self.current_file_path, "wb")
        self.writer = McapWriter(self._f)
        self.writer.start(profile=self.profile)
        
        # Re-register known state to maintain ID consistency
        # 1. Re-register Schemas
        for name, (enc, data) in self.schemas.items():
            self.writer.register_schema(name=name, encoding=enc, data=data)
            
        # 2. Re-register Channels
        for topic, (schema_id, enc, meta) in self.channels.items():
            self.writer.register_channel(
                topic=topic,
                schema_id=schema_id, 
                message_encoding=enc,
                metadata=meta
            )
            
        self.part_num += 1
        self.just_rotated = True

    def register_schema(self, name, encoding, data):
        if name not in self.schemas:
            self.schemas[name] = (encoding, data)
        return self.writer.register_schema(name=name, encoding=encoding, data=data)

    def register_channel(self, topic, message_encoding, schema_id, metadata):
        if topic not in self.channels:
            self.channels[topic] = (schema_id, message_encoding, metadata)
        return self.writer.register_channel(topic=topic, message_encoding=message_encoding, schema_id=schema_id, metadata=metadata)

    def add_message(self, channel_id, log_time, publish_time, data):
        # Check size before writing
        if self.split_size > 0 and self._f.tell() > self.split_size:
            self._open_new_segment()
            
        # FIX: Use keyword arguments to ensure data and publish_time are mapped correctly
        self.writer.add_message(
            channel_id=channel_id, 
            log_time=log_time, 
            data=data, 
            publish_time=publish_time
        )

    def finish(self):
        if self.writer:
            self.writer.finish()
            self._f.close()