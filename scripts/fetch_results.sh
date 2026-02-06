#!/usr/bin/env bash
set -euo pipefail

# Default connection settings (can be overridden with env vars)
PORT_DEFAULT=49152
USER_DEFAULT=aegir
HOST_DEFAULT=141.227.131.249
REMOTE_DIR_DEFAULT=/home/restricted/polybot/polymarket-history-generator/data

PORT="${PORT:-$PORT_DEFAULT}"
USER="${USER:-$USER_DEFAULT}"
HOST="${HOST:-$HOST_DEFAULT}"
REMOTE_DIR="${REMOTE_DIR:-$REMOTE_DIR_DEFAULT}"

DEST_DIR="${1:-./data}"

SSH_OPTS="-p ${PORT}"

usage(){
  cat <<EOF
Usage: $0 [DEST_DIR]

Copies the remote directory:
  ${USER}@${HOST}:${REMOTE_DIR}

to the local destination directory (default: ./data).

You can override connection settings with environment variables:
  USER, HOST, PORT, REMOTE_DIR

Examples:
  $0              # copies into ./data
  $0 /tmp/results # copies into /tmp/results

Or override host/port inline:
  USER=alice HOST=1.2.3.4 PORT=2222 REMOTE_DIR=/path/to/data $0 /tmp/results
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

mkdir -p "$DEST_DIR"

echo "Copying from ${USER}@${HOST}:${REMOTE_DIR} -> ${DEST_DIR}"

# Use rsync if available for a faster, resumable copy; fall back to scp
if command -v rsync >/dev/null 2>&1; then
  echo "Using rsync over ssh (port ${PORT})"
  rsync -avz -e "ssh -p ${PORT}" "${USER}@${HOST}:${REMOTE_DIR%/}/" "${DEST_DIR%/}/"
else
  echo "rsync not found; falling back to scp"
  scp -r ${SSH_OPTS} "${USER}@${HOST}:${REMOTE_DIR}" "${DEST_DIR%/}/"
fi

echo "Done."
