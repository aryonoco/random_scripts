#!/usr/bin/env bash
export LC_ALL=C
set -euo pipefail
shopt -s lastpipe

########################################################
#     Configuration                                    #
########################################################

declare -A CONFIG=(
    [source_vol]="/data"
    [snap_dir]="/data/.snapshots"
    [dest_mount]="/mnt/external"
    [min_free_gb]=1
    [lock_file]="/var/lock/external-backup.lock"
    [show_progress_percent]=true
)
declare -n config=CONFIG

########################################################
#     Global Variables                                 #
########################################################

SOURCE_BASE=$(basename "${config[source_vol]}")
SNAP_NAME="${SOURCE_BASE}.$(date -u "+%Y-%m-%dT%H:%M:%SZ")"
SRC_UUID=""
DEST_UUID=""
DELTA_SIZE=""

########################################################
#     State Variables                                  #
########################################################

SNAPSHOT_CREATED=false
BACKUP_SUCCESSFUL=false

########################################################
#     Function Definitions                              #
########################################################

err() {
    echo "[$(date -u +'%Y-%m-%dT%H:%M:%SZ')] ERROR: $*" >&2
    exit 1
}

check_dependencies() {
    command -v bc &>/dev/null || err "bc calculator required but not found. Install on Debian with 'sudo apt install bc'"
    command -v pv &>/dev/null || err "pv (Pipe Viewer) required but not found. Install on Debian with 'sudo apt install pv'"
}

verify_uuids() {
    local src_output dest_output src_uuid="" dest_uuid=""
    src_output=$(btrfs subvolume show "$1")
    dest_output=$(btrfs subvolume show "$2")
    
    src_uuid=$(grep -i "uuid:" <<< "$src_output" | head -1 | awk '{print $2}')
    dest_uuid=$(grep -i "received uuid:" <<< "$dest_output" | awk '{print $3}')
    
    SRC_UUID="$src_uuid"
    DEST_UUID="$dest_uuid"
    
    [[ "$src_uuid" == "$dest_uuid" ]]
}

format_bytes() {
    local -i bytes=$1
    local units=("B" "KB" "MB" "GB" "TB")
    local -i unit=0
    
    (( bytes == 0 )) && { echo "0 B"; return; }
    
    while (( bytes >= 1024 && unit < ${#units[@]}-1 )); do
        ((bytes /= 1024))
        ((unit++))
    done
    
    printf "%d %s" "$bytes" "${units[unit]}"
}

convert_to_bytes() {
    [[ $1 =~ ^([0-9.]+)([[:alpha:]]+)$ ]] || return 1
    local value=${BASH_REMATCH[1]}
    local unit=${BASH_REMATCH[2]^^}

    case "$unit" in
        "B")   factor=1 ;;
        "KB")  factor=1024 ;;
        "MB")  factor=$((1024**2)) ;;
        "GB")  factor=$((1024**3)) ;;
        "TB")  factor=$((1024**4)) ;;
        *)      factor=1 ;;
    esac
    
    echo $(( (${value%.*} * 10#${value#*.} * factor) / (${value#*.} > 0 ? 1 : 1) ))
}

check_destination_space() {
    local required_bytes=$1
    local dest_mount="${config[dest_mount]}"
    local buffer=$((1024**3))
    local required_with_buffer=$((required_bytes + buffer))
    
    echo "Checking destination free space on ${dest_mount@Q}..."
    
    local btrfs_output
    btrfs_output=$(btrfs filesystem usage -b "$dest_mount") || err "Failed to check destination filesystem"
    
    [[ "$btrfs_output" =~ Free\ \(estimated\):[[:space:]]+([0-9]+) ]] && free_bytes=${BASH_REMATCH[1]}
    [[ -n "$free_bytes" ]] || err "Could not parse free space"
    
    echo "Destination space status:"
    echo " - Btrfs estimated free: $(format_bytes "$free_bytes")"
    echo " - Required space: $(format_bytes "$required_with_buffer") (including 1GB buffer)"
    
    ((free_bytes < required_with_buffer)) && err "Insufficient space for backup (needs $(format_bytes $required_with_buffer))"
    
    echo "Space check passed - sufficient free space available"
    return 0
}

acquire_lock() {
    local original_umask=$(umask)
    umask 0177
    exec 9>"${config[lock_file]}"
    umask "$original_umask"

    flock -n 9 || err "Another backup is already in progress"
    printf "%d\n" $$ >&9
    flock -x 9
}

check_directory() {
    [[ -d "${config[snap_dir]}" ]] || err "Snapshot directory ${config[snap_dir]@Q} missing"
}

check_mount() {
    local config_key="$1"
    local mount_path="${config[$config_key]}"
    
    mountpoint -q "$mount_path" && return 0
    
    echo "Attempting to mount ${mount_path@Q}..." >&2
    mount "$mount_path" || err "Failed to mount ${mount_path@Q}"
    mountpoint -q "$mount_path" || err "Mount verification failed for ${mount_path@Q}"
}

find_parent_snapshot() {
    local -a snapshots
    find "${config[snap_dir]}" -maxdepth 1 -name "${SOURCE_BASE}.*" -printf '%T@ %p\0' |
        sort -znr |
        mapfile -d '' -t snapshots

    [[ ${#snapshots[@]} -eq 0 ]] && { echo "No existing snapshots found in ${config[snap_dir]@Q}" >&2; return 1; }

    for entry in "${snapshots[@]}"; do
        local snap_path="${entry#* }"
        [[ "${snap_path}" != "${config[snap_dir]}/${SNAP_NAME}" ]] && {
            echo "${snap_path#"${config[snap_dir]}/"}"
            return 0
        }
    done
    
    echo "Only found the current snapshot, no valid parent snapshot available" >&2
    return 1
}

parse_transaction_id() {
    [[ "$1" =~ (transid\ marker\ was|transaction\ id:)\ +([0-9]+) ]] && echo "${BASH_REMATCH[2]}"
    [[ -z "${BASH_REMATCH:-}" ]] && echo ""
}

estimate_delta_size() {
    local parent_path="$1" current_path="$2"
    
    echo "Calculating incremental size using btrfs dry-run..." >&2
    local total_bytes
    
    set +e
    total_bytes=$(btrfs send -p "$parent_path" --no-data "$current_path" | wc -c)
    set -e
    
    if [[ ! "$total_bytes" =~ ^[0-9]+$ ]] || (( total_bytes == 0 )); then
        echo "Warning: Dry-run byte count failed, using subvol percentage" >&2
        subvol_size=$(du -sb "$current_path" 2>/dev/null | cut -f1) || err "Fallback size estimation failed for path '${current_path}'\nPossible reasons:\n1. Snapshot path does not exist\n2. Disk I/O error\n3. Permission denied"
        [[ "$subvol_size" =~ ^[0-9]+$ ]] || err "Invalid subvolume size '${subvol_size}' from du command"
        
        total_bytes=$(( subvol_size / 10 ))
    fi

    local -i estimated_size=$(( total_bytes + (total_bytes * 5 / 100) ))
    (( estimated_size < 10485760 )) && estimated_size=10485760
    
    echo "Estimated incremental size: $(format_bytes "$estimated_size")" >&2
    echo "$estimated_size"
}

cleanup() {
    local exit_code=$?
    echo "Performing cleanup..." >&2

    [[ -n "${temp_dir:-}" && -d "$temp_dir" ]] && rm -rf "${temp_dir@Q}"
    [[ -n "${temp_path_file:-}" && -f "$temp_path_file" ]] && rm -f "${temp_path_file@Q}"

    if [[ "$SNAPSHOT_CREATED" == "true" && "$BACKUP_SUCCESSFUL" != "true" ]]; then
        echo "Backup failed or was interrupted. Removing snapshots..." >&2
        
        # Remove source snapshot
        [[ -d "${config[snap_dir]}/$SNAP_NAME" ]] && {
            local retries=3
            while (( retries-- > 0 )); do
                btrfs subvolume delete "${config[snap_dir]}/$SNAP_NAME" >/dev/null 2>&1 && {
                    echo "Successfully removed source snapshot ${SNAP_NAME@Q}" >&2
                    break
                }
                echo "WARNING: Failed to remove source snapshot (${retries} retries left)..." >&2
                sleep 1
            done
            (( retries < 0 )) && echo "ERROR: Permanent failure removing source snapshot!" >&2
        }
        
        # Remove destination snapshot
        [[ -d "${config[dest_mount]}/$SNAP_NAME" ]] && {
            echo "Removing destination snapshot ${config[dest_mount]}/$SNAP_NAME..." >&2
            local dest_retries=3
            while (( dest_retries-- > 0 )); do
                btrfs subvolume delete "${config[dest_mount]}/$SNAP_NAME" >/dev/null 2>&1 && {
                    echo "Successfully removed destination snapshot ${SNAP_NAME@Q}" >&2
                    break
                }
                echo "WARNING: Failed to remove destination snapshot (${dest_retries} retries left)..." >&2
                sleep 1
            done
            (( dest_retries < 0 )) && echo "ERROR: Permanent failure removing destination snapshot!" >&2
        }
    fi
    
    exit $exit_code
}

########################################################
#     Main Process                                      #
########################################################

# Setup error handling
trap 'cleanup' EXIT INT TERM HUP
# Validate environment
check_dependencies
check_directory
check_mount "source_vol"
check_mount "dest_mount"
# Acquire lock
acquire_lock
# Create snapshot
btrfs subvolume snapshot -r "${config[source_vol]}" "${config[snap_dir]}/$SNAP_NAME" && SNAPSHOT_CREATED=true || err "Failed to create snapshot ${SNAP_NAME@Q}"
echo "Created snapshot: ${SNAP_NAME@Q}"
# Verify snapshot
btrfs subvolume show "${config[snap_dir]}/$SNAP_NAME" >/dev/null || err "Failed to verify snapshot ${SNAP_NAME@Q}"
# Find parent snapshot
PARENT_SNAP=$(find_parent_snapshot) || { echo "No parent snapshot available, will perform full backup" >&2; PARENT_SNAP=""; }
# Perform backup
if [[ -n "$PARENT_SNAP" ]]; then
    echo "Verifying parent snapshot on destination..."
    [[ -d "${config[dest_mount]}/$PARENT_SNAP" ]] || err "Parent snapshot ${PARENT_SNAP@Q} missing from destination"
    
    verify_uuids "${config[snap_dir]}/$PARENT_SNAP" "${config[dest_mount]}/$PARENT_SNAP" || err "Parent snapshot UUID mismatch - possible corruption!\nSource UUID: $SRC_UUID\nDest UUID:   $DEST_UUID"
    
    echo "Calculating incremental size..."
    DELTA_SIZE=$(estimate_delta_size "${config[snap_dir]}/$PARENT_SNAP" "${config[snap_dir]}/$SNAP_NAME")
    
    check_destination_space "$DELTA_SIZE" || { BACKUP_SUCCESSFUL=false; err "Aborting backup due to insufficient space"; }
    
    echo "Starting incremental send with progress monitoring"
    set +e
    btrfs send -p "${config[snap_dir]}/$PARENT_SNAP" \
        "${config[snap_dir]}/$SNAP_NAME" | \
        pv -petab | \
        btrfs receive "${config[dest_mount]}/" 2>&1 | grep -v 'write .* offset='
    declare -a pipestatus=("${PIPESTATUS[@]}")
    set -e
    (( pipestatus[0] != 0 )) && err "btrfs send failed with code ${pipestatus[0]}"
    (( pipestatus[1] != 0 )) && err "Progress monitor (pv) failed with code ${pipestatus[1]}"
    (( pipestatus[2] != 0 )) && err "btrfs receive failed with code ${pipestatus[2]}"
    
    echo "Verifying destination snapshot integrity..."
    verify_uuids "${config[snap_dir]}/$SNAP_NAME" "${config[dest_mount]}/$SNAP_NAME" || {
        err "Destination snapshot UUID mismatch - possible corruption detected\nSource UUID: $SRC_UUID\nDestination UUID: $DEST_UUID"
    }

    BACKUP_SUCCESSFUL=true
    echo "Backup successful: ${SNAP_NAME@Q} ($([[ -n "$PARENT_SNAP" ]] && echo "incremental" || echo "full"))!"
else
    echo "Calculating full backup size..."
    local btrfs_show_output
    btrfs_show_output=$(btrfs subvolume show "${config[snap_dir]}/$SNAP_NAME")
    [[ "$btrfs_show_output" =~ Total\ bytes:[[:space:]]+([0-9,]+) ]] && DELTA_SIZE=${BASH_REMATCH[1]//,/}
    
    [[ "$DELTA_SIZE" =~ ^[0-9]+$ ]] || DELTA_SIZE=$(du -sb "${config[snap_dir]}/$SNAP_NAME" | cut -f1)
    
    check_destination_space "$DELTA_SIZE" || { BACKUP_SUCCESSFUL=false; err "Aborting backup due to insufficient space"; }
    
    echo "Starting full send with progress monitoring"
    set +e
    btrfs send "${config[snap_dir]}/$SNAP_NAME" | \
        pv -petab | \
        btrfs receive "${config[dest_mount]}/" 2>&1 | grep -v 'write .* offset='
    declare -a pipestatus=("${PIPESTATUS[@]}")
    set -e
    (( pipestatus[0] != 0 )) && err "btrfs send failed with code ${pipestatus[0]}"
    (( pipestatus[1] != 0 )) && err "Progress monitor (pv) failed with code ${pipestatus[1]}"
    (( pipestatus[2] != 0 )) && err "btrfs receive failed with code ${pipestatus[2]}"
    
    echo "Verifying destination snapshot integrity..."
    verify_uuids "${config[snap_dir]}/$SNAP_NAME" "${config[dest_mount]}/$SNAP_NAME" || {
        err "Destination snapshot UUID mismatch - possible corruption detected\nSource UUID: $SRC_UUID\nDestination UUID: $DEST_UUID"
    }

    BACKUP_SUCCESSFUL=true
    echo "Backup successful: ${SNAP_NAME@Q} ($([[ -n "$PARENT_SNAP" ]] && echo "incremental" || echo "full"))!"
fi