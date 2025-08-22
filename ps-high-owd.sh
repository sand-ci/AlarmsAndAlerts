#!/bin/bash
# Run the high one-way delay detection script.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# If you use a virtual environment, uncomment and set the path:
# source /path/to/venv/bin/activate

python ps-high-owd.py "$@"
