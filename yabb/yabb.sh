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
  --allow-run=btrfs,mount,mountpoint,find,pv,du,which,test,lsblk,blkid \
  --allow-read=/data,/mnt/external,/var/lock,/usr/bin,/etc/mtab,/dev,/proc,/sys \
  --allow-write=/data/.snapshots,/mnt/external,/var/lock,/tmp \
  --allow-env=TZ,HOME,USER \
  --allow-net=jsr.io,registry.npmjs.org,cdn.jsdelivr.net \
  --allow-sys \
  --allow-ffi \
  --no-check \
  --no-prompt \
  "$TS_FILE" "$@"
