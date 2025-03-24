#!/usr/bin/env bash
# YABB - Yet Another BTRFS Backup - Script Launcher

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
TS_FILE="${SCRIPT_DIR}/yabb.ts"

# Validate TypeScript file exists
if [ ! -f "$TS_FILE" ]; then
  echo "Error: Could not find yabb.ts file at $TS_FILE"
  exit 1
fi

# Run Deno with all required permissions and flags
exec deno run \
  --allow-run=btrfs,mount,mountpoint,find,pv,du,which,test \
  --allow-read=/data,/mnt/external,/var/lock,/usr/bin,/etc/mtab \
  --allow-write=/data/.snapshots,/mnt/external,/var/lock \
  --allow-env=TZ \
  --allow-net=jsr.io \
  --allow-sys \
  --unstable-kv \
  --v8-flags="--max-old-space-size=256,--jitless,--optimize-for-size,--use-ic,--no-concurrent-recompilation,--enable-ssse3,--enable-sse4-1,--enable-sse4-2" \
  --no-check "$TS_FILE" "$@"
