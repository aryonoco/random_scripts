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

# Logging functions
readonly LOG_TIMESTAMP_FORMAT='%Y-%m-%dT%H:%M:%SZ'

log_info() {
    echo "[$(date -u +"$LOG_TIMESTAMP_FORMAT")] INFO: $*"
}

log_warn() {
    echo "[$(date -u +"$LOG_TIMESTAMP_FORMAT")] WARN: $*" >&2
}

log_error() {
    echo "[$(date -u +"$LOG_TIMESTAMP_FORMAT")] ERROR: $*" >&2
}

die() {
    log_error "$@"
    exit 1
}

check_dependencies() {
    command -v bc &>/dev/null || die "bc calculator required but not found. Install on Debian with 'sudo apt install bc'"
    command -v pv &>/dev/null || die "pv (Pipe Viewer) required but not found. Install on Debian with 'sudo apt install pv'"
}

verify_uuids() {
    local src_output dest_output src_uuid="" dest_uuid=""
    src_output=$(btrfs subvolume show "$1")
    dest_output=$(btrfs subvolume show "$2")

    src_uuid=$(grep -i "uuid:" <<< "$src_output" | head -1 | awk '{print $2}')
    dest_uuid=$(grep -i "received uuid:" <<< "$dest_output" | awk '{print $3}')

    SRC_UUID="$src_uuid"
    DEST_UUID="$dest_uuid"

    # Validate that both UUIDs were successfully extracted
    [[ -z "$src_uuid" || -z "$dest_uuid" ]] && return 1

    [[ "$src_uuid" == "$dest_uuid" ]]
}

format_bytes() {
    local bytes=$1
    local units=("B" "KB" "MB" "GB" "TB")
    local unit=0
    local result

    (( bytes == 0 )) && { echo "0 B"; return; }
    while (( unit < ${#units[@]}-1 )); do
        result=$(echo "scale=2; $bytes / 1024" | bc)
        if (( $(echo "$result >= 1" | bc) == 1 )); then
            bytes=$result
            ((unit++))
        else
            break
        fi
    done

    if [[ "$bytes" =~ \. ]]; then
        printf "%.1f %s" "$bytes" "${units[unit]}"
    else
        printf "%.0f %s" "$bytes" "${units[unit]}"
    fi
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
        *)     factor=1 ;;
    esac

    printf "%.0f\n" "$(echo "$value * $factor" | bc)"
}

check_destination_space() {
    local required_bytes=$1
    local dest_mount="${config[dest_mount]}"
    local buffer=$(convert_to_bytes "${config[min_free_gb]}GB")
    local required_with_buffer=$((required_bytes + buffer))
    local free_bytes=""  # Declare as local and initialize

    log_info "Checking destination free space on ${dest_mount@Q}..."

    local btrfs_output
    btrfs_output=$(btrfs filesystem usage -b "$dest_mount") || die "Failed to check destination filesystem"

    # Parse free space from btrfs output
    if [[ "$btrfs_output" =~ Free\ \(estimated\):[[:space:]]+([0-9]+) ]]; then
        free_bytes=${BASH_REMATCH[1]}
    else
        die "Could not parse free space from btrfs output"
    fi

    # Validate that free_bytes contains a valid number
    if [[ ! "$free_bytes" =~ ^[0-9]+$ ]]; then
        die "Invalid free space value parsed: '$free_bytes'"
    fi

    log_info "Destination space status:"
    log_info " - Btrfs estimated free: $(format_bytes "$free_bytes")"
    log_info " - Required space: $(format_bytes "$required_with_buffer") (including ${config[min_free_gb]}GB buffer)"

    ((free_bytes < required_with_buffer)) && die "Insufficient space for backup (needs $(format_bytes $required_with_buffer))"

    log_info "Space check passed - sufficient free space available"
    return 0
}

acquire_lock() {
    local original_umask=$(umask)
    umask 0177
    exec 9>"${config[lock_file]}"
    umask "$original_umask"

    flock -n 9 || die "Another backup is already in progress"
    printf "%d\n" $$ >&9 || die "Failed to write PID to lock file"
    flock -x 9
}

check_directory() {
    [[ -d "${config[snap_dir]}" ]] || die "Snapshot directory ${config[snap_dir]@Q} missing"
}

check_mount() {
    local config_key="$1"
    local mount_path="${config[$config_key]}"

    mountpoint -q "$mount_path" && return 0

    log_info "Attempting to mount ${mount_path@Q}..."
    mount "$mount_path" || die "Failed to mount ${mount_path@Q}"
    mountpoint -q "$mount_path" || die "Mount verification failed for ${mount_path@Q}"
}

find_parent_snapshot() {
    local -a snapshots
    find "${config[snap_dir]}" -maxdepth 1 -name "${SOURCE_BASE}.*" -printf '%T@ %p\0' |
        sort -znr |
        mapfile -d '' -t snapshots

    [[ ${#snapshots[@]} -eq 0 ]] && { log_warn "No existing snapshots found in ${config[snap_dir]@Q}"; return 1; }

    for entry in "${snapshots[@]}"; do
        local snap_path="${entry#* }"
        [[ "${snap_path}" != "${config[snap_dir]}/${SNAP_NAME}" ]] && {
            echo "${snap_path#"${config[snap_dir]}/"}"
            return 0
        }
    done

    log_warn "Only found the current snapshot, no valid parent snapshot available"
    return 1
}

delete_snapshot() {
    local snapshot_path="$1"
    local description="$2"

    log_info "Removing $description snapshot ${snapshot_path}..."

    # Try normal delete first
    if btrfs subvolume delete "$snapshot_path" 2>/dev/null; then
        log_info "Successfully removed $description snapshot"
        return 0
    fi

    # Try with -c flag for partial/corrupted snapshots
    if btrfs subvolume delete -c "$snapshot_path" 2>/dev/null; then
        log_info "Successfully removed $description snapshot (with -c flag)"
        return 0
    fi

    log_warn "Failed to remove $description snapshot"
    return 1
}

cleanup_partial_snapshot() {
    if [[ -d "${config[dest_mount]}/$SNAP_NAME" ]]; then
        log_warn "Partial snapshot exists at destination, removing..."
        delete_snapshot "${config[dest_mount]}/$SNAP_NAME" "partial" || \
            die "Cannot remove partial snapshot at ${config[dest_mount]}/$SNAP_NAME"
    fi
}

calculate_backup_size() {
    local backup_type="$1"

    if [[ "$backup_type" == "incremental" ]]; then
        log_info "Skipping size estimation for incremental backup (Btrfs limitation)"
        echo "104857600"  # 100MB minimum
    else
        log_info "Calculating full backup size..."
        local btrfs_show_output
        btrfs_show_output=$(btrfs subvolume show "${config[snap_dir]}/$SNAP_NAME" 2>/dev/null) || true

        # Try multiple patterns to be more resilient to format changes
        local size=""
        if [[ "$btrfs_show_output" =~ Total\ bytes:[[:space:]]+([0-9,]+) ]]; then
            size=${BASH_REMATCH[1]//,/}
        elif [[ "$btrfs_show_output" =~ [Tt]otal[[:space:]]+[Bb]ytes:[[:space:]]+([0-9,]+) ]]; then
            size=${BASH_REMATCH[1]//,/}
        fi

        # Fallback to du if btrfs parsing fails
        if [[ ! "$size" =~ ^[0-9]+$ ]]; then
            log_warn "Could not parse btrfs output, falling back to du"
            size=$(du -sb "${config[snap_dir]}/$SNAP_NAME" | cut -f1)
        fi

        echo "$size"
    fi
}

execute_backup_pipeline() {
    local backup_type="$1"
    local parent_snap="${2:-}"  # Optional parent snapshot for incremental

    log_info "Starting $backup_type send with progress monitoring"

    local receive_marker="/tmp/.btrfs-receive-$$-$SNAP_NAME"
    local error_log="/tmp/.btrfs-error-$$-$SNAP_NAME"

    # Build command array based on backup type
    local -a send_cmd
    if [[ "$backup_type" == "incremental" && -n "$parent_snap" ]]; then
        send_cmd=(
            btrfs send
            -p "${config[snap_dir]}/$parent_snap"
            "${config[snap_dir]}/$SNAP_NAME"
        )
    else
        send_cmd=(
            btrfs send
            "${config[snap_dir]}/$SNAP_NAME"
        )
    fi

    # Execute pipeline
    local pipeline_result=0
    "${send_cmd[@]}" 2>"$error_log.send" | \
        pv -petab 2>"$error_log.pv" | \
        { touch "$receive_marker"; btrfs receive "${config[dest_mount]}/" 2>"$error_log.receive"; } | \
        grep -v 'write .* offset=' || pipeline_result=$?

    local send_status=${PIPESTATUS[0]:-0}
    local pv_status=${PIPESTATUS[1]:-0}
    local receive_status=${PIPESTATUS[2]:-0}
    local grep_status=${PIPESTATUS[3]:-0}

    local receive_started=false
    [[ -f "$receive_marker" ]] && receive_started=true
    rm -f "$receive_marker"

    if (( send_status != 0 )); then
        [[ -s "$error_log.send" ]] && log_error "Send error details:" && cat "$error_log.send" >&2
        if [[ "$receive_started" == "true" && -d "${config[dest_mount]}/$SNAP_NAME" ]]; then
            log_info "Send failed, removing partial destination snapshot..."
            delete_snapshot "${config[dest_mount]}/$SNAP_NAME" "partial destination" || true
        fi
        rm -f "$error_log".*
        die "btrfs send failed with code ${send_status}"
    fi

    if (( receive_status != 0 )); then
        [[ -s "$error_log.receive" ]] && log_error "Receive error details:" && cat "$error_log.receive" >&2
        if [[ -d "${config[dest_mount]}/$SNAP_NAME" ]]; then
            log_info "Receive failed, removing partial destination snapshot..."
            delete_snapshot "${config[dest_mount]}/$SNAP_NAME" "partial destination" || true
        fi
        rm -f "$error_log".*
        die "btrfs receive failed with code ${receive_status}"
    fi

    if (( pv_status != 0 )); then
        [[ -s "$error_log.pv" ]] && log_error "Progress monitor error:" && cat "$error_log.pv" >&2
        rm -f "$error_log".*
        die "Progress monitor (pv) failed with code ${pv_status}"
    fi

    # Clean up error logs if successful
    rm -f "$error_log".*
}

finalize_backup() {
    local backup_type="$1"

    log_info "Verifying destination snapshot integrity..."
    verify_uuids "${config[snap_dir]}/$SNAP_NAME" "${config[dest_mount]}/$SNAP_NAME" || {
        die "Destination snapshot UUID mismatch - possible corruption detected\nSource UUID: $SRC_UUID\nDestination UUID: $DEST_UUID"
    }

    BACKUP_SUCCESSFUL=true

    log_info "Syncing destination filesystem..."
    btrfs filesystem sync "${config[dest_mount]}"

    log_info "Backup successful: ${SNAP_NAME@Q} ($backup_type)!"
}

cleanup() {
    local exit_code=$?
    log_info "Performing cleanup..."

    if [[ "$SNAPSHOT_CREATED" == "true" && "$BACKUP_SUCCESSFUL" != "true" ]]; then
        log_warn "Backup failed or was interrupted. Removing snapshots..."

        # Remove source snapshot
        if [[ -d "${config[snap_dir]}/$SNAP_NAME" ]]; then
            local retries=3
            while (( retries-- > 0 )); do
                if delete_snapshot "${config[snap_dir]}/$SNAP_NAME" "source"; then
                    break
                fi
                (( retries > 0 )) && sleep 1
            done
            (( retries < 0 )) && log_error "Permanent failure removing source snapshot!"
        fi

        # Remove destination snapshot
        if [[ -d "${config[dest_mount]}/$SNAP_NAME" ]]; then
            delete_snapshot "${config[dest_mount]}/$SNAP_NAME" "destination" || \
                log_error "Could not remove destination snapshot!"
        fi
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
btrfs subvolume snapshot -r "${config[source_vol]}" "${config[snap_dir]}/$SNAP_NAME" && \
    SNAPSHOT_CREATED=true || die "Failed to create snapshot ${SNAP_NAME@Q}"
log_info "Created snapshot: ${SNAP_NAME@Q}"

# Verify snapshot
btrfs subvolume show "${config[snap_dir]}/$SNAP_NAME" >/dev/null || \
    die "Failed to verify snapshot ${SNAP_NAME@Q}"

# Find parent snapshot
PARENT_SNAP=$(find_parent_snapshot) || {
    log_info "No parent snapshot available, will perform full backup"
    PARENT_SNAP=""
}

# Perform backup based on whether parent exists
if [[ -n "$PARENT_SNAP" ]]; then
    # Incremental backup
    log_info "Verifying parent snapshot on destination..."
    [[ -d "${config[dest_mount]}/$PARENT_SNAP" ]] || \
        die "Parent snapshot ${PARENT_SNAP@Q} missing from destination"

    verify_uuids "${config[snap_dir]}/$PARENT_SNAP" "${config[dest_mount]}/$PARENT_SNAP" || \
        die "Parent snapshot UUID mismatch - possible corruption!\nSource UUID: $SRC_UUID\nDest UUID: $DEST_UUID"

    cleanup_partial_snapshot

    # Calculate space needed
    log_info "Checking minimal space requirements for incremental backup..."
    DELTA_SIZE=$(calculate_backup_size "incremental")
    check_destination_space "$DELTA_SIZE" || {
        BACKUP_SUCCESSFUL=false
        die "Aborting backup due to insufficient space"
    }

    # Execute incremental backup
    execute_backup_pipeline "incremental" "$PARENT_SNAP"

    finalize_backup "incremental"
else
    # Full backup
    cleanup_partial_snapshot
    DELTA_SIZE=$(calculate_backup_size "full")
    check_destination_space "$DELTA_SIZE" || {
        BACKUP_SUCCESSFUL=false
        die "Aborting backup due to insufficient space"
    }

    execute_backup_pipeline "full"

    finalize_backup "full"
fi
