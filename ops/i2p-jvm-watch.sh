#!/bin/bash
# i2p-jvm-watch.sh — detect JVM restarts inside the I2P service wrapper.
#
# Background: on the Debian/Ubuntu i2p package, systemd's Main PID is the
# Tanuki service wrapper, not the router JVM. If the JVM exits or hangs,
# the wrapper restarts it internally — systemd sees the same PID,
# NRestarts stays 0, and the only visible symptom is the router console
# uptime resetting. That blind spot hid a restart for 3 days in Oct 2026.
#
# This script polls the JVM's process start time (via the wrapper's child
# PID). A change means the JVM restarted. Restarts are appended to
# /var/log/i2p-watch.log with old/new start times; the first run only
# records a baseline. Intended to run from root cron every 30 minutes.
set -u

STATE_DIR=/var/lib/i2p-watch
STATE_FILE="$STATE_DIR/last_jvm_start"
LOG_FILE=/var/log/i2p-watch.log

mkdir -p "$STATE_DIR"

WPID=$(systemctl show i2p -p MainPID --value 2>/dev/null)
[ -z "$WPID" ] && exit 0

JVM_PID=$(pgrep -P "$WPID" 2>/dev/null | head -1)
[ -z "$JVM_PID" ] && exit 0

START=$(ps -o lstart= -p "$JVM_PID" 2>/dev/null | awk '{$1=$1};1')
[ -z "$START" ] && exit 0

NOW=$(date '+%Y-%m-%d %H:%M:%S %Z')

if [ ! -f "$STATE_FILE" ]; then
    echo "$START" > "$STATE_FILE"
    echo "$NOW baseline: JVM started $START (pid $JVM_PID)" >> "$LOG_FILE"
    exit 0
fi

PREV=$(cat "$STATE_FILE")
if [ "$START" != "$PREV" ]; then
    echo "$NOW RESTART DETECTED: JVM previously started $PREV, now started $START (pid $JVM_PID)" >> "$LOG_FILE"
    echo "$START" > "$STATE_FILE"
fi

exit 0
