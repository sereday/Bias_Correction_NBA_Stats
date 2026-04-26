import json
from pathlib import Path


def load(path="job_request.json"):
    with open(Path(path)) as f:
        return json.load(f)
