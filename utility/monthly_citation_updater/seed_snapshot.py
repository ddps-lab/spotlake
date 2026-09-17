"""Create the initial object only; never overwrite a newer monthly snapshot."""
import json
import subprocess
import sys
from pathlib import Path
from lambda_function import validate_snapshot

seed = Path(sys.argv[1]).resolve()
validate_snapshot(json.loads(seed.read_text()))
result = subprocess.run([
    "aws", "s3api", "put-object", "--bucket", "spotlake-public-daily",
    "--key", "citations/citations.json", "--body", str(seed),
    "--content-type", "application/json; charset=utf-8", "--cache-control", "public, max-age=3600",
    "--if-none-match", "*",
], capture_output=True, text=True)
if result.returncode and "(PreconditionFailed)" not in result.stderr:
    raise SystemExit(result.stderr)
print("Existing snapshot retained" if result.returncode else "Initial snapshot created")
