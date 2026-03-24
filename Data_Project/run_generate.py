import subprocess
import sys

cmd = [
    sys.executable,
    "src/gen/event_generator.py",
    "--schema_dir", "schemas",
    "--out_dir", "data/out",
    "--players", "150",
    "--matches", "3000",
    "--seed", "42",
    "--start_time", "2026-01-01T10:00:00Z"
]
subprocess.run(cmd, check=True)