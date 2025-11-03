#!/usr/bin/env bash
set -euo pipefail

LOGDIR="${HOME}/logs"
CFG="${HOME}/.cloudflared/viewer.yml"
mkdir -p "${LOGDIR}"

# Look for a running cloudflared process using our config
if pgrep -af "cloudflared.*${CFG}" >/dev/null; then
    echo "✅ Viewer tunnel already running."
else
    echo "🚀 Starting Viewer tunnel..."
    nohup cloudflared tunnel --config "${CFG}" run \
        &> "${LOGDIR}/cloudflared-viewer.log" &
    sleep 2
    if pgrep -af "cloudflared.*${CFG}" >/dev/null; then
        echo "✅ Viewer tunnel started successfully. Logs at ${LOGDIR}/cloudflared-viewer.log"
    else
        echo "❌ Failed to start Viewer tunnel. Check logs at ${LOGDIR}/cloudflared-viewer.log"
    fi
fi

