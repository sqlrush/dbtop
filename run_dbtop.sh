#!/bin/bash
# Copyright (c) ailinkdb. All rights reserved.
# Author: sqlrush
#
# DBTOP - Oracle Database Real-time Monitor
# Usage: ./run_dbtop.sh [options]
#   -u USER          Database user (default: system)
#   -H HOST          Database host (default: localhost)
#   -p PORT          Listener port (default: 1521)
#   -s SERVICE_NAME  Oracle service name (default: orcl)
#   -i INTERVAL      Refresh interval in seconds (default: 3)
#   -d               Run in daemon mode

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if command -v dbtop &>/dev/null; then
    cd "$SCRIPT_DIR"
    dbtop "$@"
else
    cd "$SCRIPT_DIR"
    python3 -m tool.dbtop "$@"
fi
