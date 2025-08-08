#!/usr/bin/env bash
# YABB - Yet Another BTRFS Backup
#
export LC_ALL=C
set -euo pipefail
shopt -s lastpipe

########################################################
#     Configuration                                    #
########################################################

declare -A CONFIG=(
    [source_vol]="/data"
    [dest_mount]="/mnt/external"
    [min_free_gb]=1
    [lock_file]="/var/lock/yabb.lock"
    [retention_days]=30        # Delete snapshots older than this (0 = disabled)
    [keep_minimum]=5           # Always keep at least this many snapshots
    [verify_sample_percent]=5  # Percentage of files to sample for checksum verification
    [minimum_days_between_scrubs]=30  # Minimum days between automatic scrubs (0 = disabled)
    [scrub_rate_limit]=""      # Optional rate limit for scrub (e.g., "100M")
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

# Derive snapshot directories from source and destination
SNAP_DIR="${config[source_vol]}/.yabb_snapshots"
DEST_SNAP_DIR="${config[dest_mount]}/.yabb_snapshots"

# Device error tracking for verification
PRE_BACKUP_ERRORS=0
POST_BACKUP_ERRORS=0

########################################################
#     State Variables                                  #
########################################################

SNAPSHOT_CREATED=false
BACKUP_SUCCESSFUL=false
VERIFICATION_PASSED=true  # Track verification status

########################################################
#     Function Definitions                              #
########################################################

# Logging functions
readonly LOG_TIMESTAMP_FORMAT='%Y-%m-%dT%H:%M:%SZ'

log_info() {
    echo "[$(date -u +"$LOG_TIMESTAMP_FORMAT")] YABB INFO: $*"
}

log_warn() {
    echo "[$(date -u +"$LOG_TIMESTAMP_FORMAT")] YABB WARN: $*" >&2
}

log_error() {
    echo "[$(date -u +"$LOG_TIMESTAMP_FORMAT")] YABB ERROR: $*" >&2
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

# Extract epoch timestamp from snapshot name
get_snapshot_epoch() {
    local snap_name="$1"
    # Extract timestamp from name like: data.2024-01-15T14:30:00Z
    local timestamp="${snap_name#*.}"  # Remove prefix
    date -d "${timestamp/T/ }" +%s 2>/dev/null || echo 0
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
}

ensure_snapshot_directories() {
    # Ensure source snapshot directory exists
    if [[ ! -d "$SNAP_DIR" ]]; then
        log_info "Creating source snapshot directory: ${SNAP_DIR@Q}"
        mkdir -p "$SNAP_DIR" || die "Failed to create snapshot directory ${SNAP_DIR@Q}"
    fi

    # Ensure destination snapshot directory exists
    if [[ ! -d "$DEST_SNAP_DIR" ]]; then
        log_info "Creating destination snapshot directory: ${DEST_SNAP_DIR@Q}"
        mkdir -p "$DEST_SNAP_DIR" || die "Failed to create destination snapshot directory ${DEST_SNAP_DIR@Q}"
    fi
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
    find "$SNAP_DIR" -maxdepth 1 -name "${SOURCE_BASE}.*" -printf '%T@ %p\0' |
        sort -znr |
        mapfile -d '' -t snapshots

    [[ ${#snapshots[@]} -eq 0 ]] && { log_warn "No existing snapshots found in ${SNAP_DIR@Q}"; return 1; }

    for entry in "${snapshots[@]}"; do
        local snap_path="${entry#* }"
        [[ "${snap_path}" != "$SNAP_DIR/${SNAP_NAME}" ]] && {
            echo "${snap_path#"$SNAP_DIR/"}"
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
    if [[ -d "$DEST_SNAP_DIR/$SNAP_NAME" ]]; then
        log_warn "Partial snapshot exists at destination, removing..."
        delete_snapshot "$DEST_SNAP_DIR/$SNAP_NAME" "partial" || \
            die "Cannot remove partial snapshot at $DEST_SNAP_DIR/$SNAP_NAME"
    fi
}

########################################################
#     Verification Functions                           #
########################################################

# Check device error statistics
check_device_errors() {
    local mount_point="$1"
    local phase="$2"  # "pre-backup" or "post-backup"

    log_info "Checking device error statistics ($phase)..."

    # Get device stats and check for non-zero errors
    local stats_output exit_code
    stats_output=$(btrfs device stats --check "$mount_point" 2>&1)
    exit_code=$?

    if [[ $exit_code -ne 0 ]]; then
        log_warn "Device errors detected on $mount_point:"
        btrfs device stats "$mount_point" 2>&1 | grep -v " 0$" >&2

        # Store error counts for comparison
        if [[ "$phase" == "pre-backup" ]]; then
            PRE_BACKUP_ERRORS=$exit_code
        else
            POST_BACKUP_ERRORS=$exit_code
            if [[ $POST_BACKUP_ERRORS -gt ${PRE_BACKUP_ERRORS:-0} ]]; then
                log_error "New device errors occurred during backup!"
                VERIFICATION_PASSED=false
                return 1
            fi
        fi
    fi

    return 0
}

# Verify backup integrity using BTRFS scrub
verify_backup_with_scrub() {
    local snapshot_path="$1"
    local description="$2"

    log_info "Starting integrity verification via scrub for $description..."

    # Build scrub command with optional rate limiting
    local -a scrub_cmd=(btrfs scrub start -B)
    [[ -n "${config[scrub_rate_limit]}" ]] && scrub_cmd+=(-r "${config[scrub_rate_limit]}")
    scrub_cmd+=("$snapshot_path")

    # Start scrub on the specific snapshot path
    local scrub_output
    if scrub_output=$("${scrub_cmd[@]}" 2>&1); then
        # Parse scrub results - look for error summary
        local error_summary
        error_summary=$(echo "$scrub_output" | grep -E "Error summary:" || echo "Error summary: no errors found")

        if [[ "$error_summary" =~ "no errors found" ]] || [[ "$error_summary" =~ " 0 " ]]; then
            log_info "Scrub verification passed - no errors detected"
            return 0
        else
            log_error "Scrub detected errors in backup: $error_summary"
            VERIFICATION_PASSED=false
            return 1
        fi
    else
        log_warn "Scrub verification failed: $scrub_output"
        VERIFICATION_PASSED=false
        return 1
    fi
}

# Verify data checksums by sampling files
verify_data_checksums() {
    local snapshot="$1"
    local sample_percentage="${2:-${config[verify_sample_percent]:-5}}"  # Default 5% sampling

    # Skip if sampling is disabled
    [[ "$sample_percentage" -eq 0 ]] && return 0

    log_info "Verifying data integrity via checksum reads (${sample_percentage}% sample)..."

    local total_files verified_files=0 failed_files=0

    # Count total files
    total_files=$(find "$snapshot" -type f 2>/dev/null | wc -l)
    [[ "$total_files" -eq 0 ]] && { log_info "No files to verify in snapshot"; return 0; }

    local sample_size=$((total_files * sample_percentage / 100))
    [[ $sample_size -lt 10 && $total_files -ge 10 ]] && sample_size=10  # Minimum 10 files if available
    [[ $sample_size -gt $total_files ]] && sample_size=$total_files

    log_info "Sampling $sample_size of $total_files files for checksum verification..."

    # Sample and verify files using process substitution to avoid subshell variable issues
    local file
    while IFS= read -r file; do
        ((verified_files++))

        # Reading the file triggers BTRFS checksum verification
        if ! dd if="$file" of=/dev/null bs=1M status=none 2>/dev/null; then
            ((failed_files++))
            log_error "Checksum verification failed for: $file"
        fi

        # Progress indication every 100 files
        if (( verified_files % 100 == 0 )); then
            log_info "Verified $verified_files/$sample_size files..."
        fi
    done < <(find "$snapshot" -type f 2>/dev/null | shuf -n "$sample_size" 2>/dev/null || find "$snapshot" -type f 2>/dev/null | head -n "$sample_size")

    if [[ $failed_files -gt 0 ]]; then
        log_error "Checksum verification failed for $failed_files files"
        VERIFICATION_PASSED=false
        return 1
    else
        log_info "Successfully verified $verified_files files (no checksum errors)"
        return 0
    fi
}

# Schedule periodic scrub if needed
schedule_periodic_scrub() {
    local mount_point="$1"
    
    # Get the minimum days between scrubs configuration
    local min_days_between_scrubs="${config[minimum_days_between_scrubs]:-30}"
    
    # Skip if periodic scrub is disabled (0 means disabled)
    [[ "$min_days_between_scrubs" -eq 0 ]] && return 0

    # Ensure state directory exists
    local state_dir="/var/lib/yabb"
    [[ -d "$state_dir" ]] || mkdir -p "$state_dir" 2>/dev/null || state_dir="/tmp"

    # Check if last scrub was more than configured days ago
    local last_scrub_file="$state_dir/.last_scrub_${mount_point//\//_}"
    local current_time=$(date +%s)
    local last_scrub_time=0

    [[ -f "$last_scrub_file" ]] && last_scrub_time=$(cat "$last_scrub_file" 2>/dev/null || echo 0)

    local days_since_scrub=$(( (current_time - last_scrub_time) / 86400 ))

    if [[ $days_since_scrub -gt $min_days_between_scrubs ]]; then
        log_info "Starting periodic scrub (last scrub: ${days_since_scrub} days ago, threshold: ${min_days_between_scrubs} days)..."

        # Build scrub command with optional rate limiting
        local -a scrub_cmd=(btrfs scrub start -B)
        [[ -n "${config[scrub_rate_limit]}" ]] && scrub_cmd+=(-r "${config[scrub_rate_limit]}")
        scrub_cmd+=("$mount_point")

        if "${scrub_cmd[@]}" 2>&1; then
            echo "$current_time" > "$last_scrub_file"
            log_info "Periodic scrub completed successfully"
        else
            log_warn "Periodic scrub encountered errors"
        fi
    fi
}

# Prune old snapshots based on retention policy
prune_old_snapshots() {
    local location="$1"  # Either snap_dir or dest_mount

    # Skip if retention disabled
    [[ "${config[retention_days]:-0}" -eq 0 ]] && return 0
    [[ ! -d "$location" ]] && return 1

    local cutoff_epoch=$(($(date +%s) - config[retention_days] * 86400))
    local -a snapshots_to_delete=()
    local -a all_snapshots=()

    # Collect all snapshots with timestamps
    while IFS= read -r -d '' snapshot; do
        local snap_name=$(basename "$snapshot")
        # Skip current snapshot being created
        [[ "$snap_name" == "$SNAP_NAME" ]] && continue

        local snap_epoch=$(get_snapshot_epoch "$snap_name")
        [[ "$snap_epoch" -eq 0 ]] && continue  # Skip if can't parse date

        all_snapshots+=("$snap_epoch:$snapshot")
    done < <(find "$location" -maxdepth 1 -name "${SOURCE_BASE}.*" -type d -print0)

    # Sort by age (oldest first)
    IFS=$'\n' sorted_snapshots=($(sort -n <<<"${all_snapshots[*]}"))
    unset IFS

    # Determine which to delete
    local total_count=${#sorted_snapshots[@]}
    local keep_count="${config[keep_minimum]:-5}"

    for snapshot_info in "${sorted_snapshots[@]}"; do
        local epoch="${snapshot_info%%:*}"
        local path="${snapshot_info#*:}"

        # Always keep minimum number regardless of age
        if (( total_count - ${#snapshots_to_delete[@]} <= keep_count )); then
            break
        fi

        # Delete if older than retention period
        if (( epoch < cutoff_epoch )); then
            snapshots_to_delete+=("$path")
        fi
    done

    # Execute deletions
    if [[ ${#snapshots_to_delete[@]} -gt 0 ]]; then
        log_info "Pruning ${#snapshots_to_delete[@]} old snapshots from $location"

        for snapshot in "${snapshots_to_delete[@]}"; do
            delete_snapshot "$snapshot" "old" || \
                log_warn "Failed to prune: $(basename "$snapshot")"
        done
    fi
}

calculate_backup_size() {
    local backup_type="$1"

    if [[ "$backup_type" == "incremental" ]]; then
        local parent_snap="${2:-}"  # Parent snapshot name passed from caller

        if [[ -z "$parent_snap" ]]; then
            log_warn "No parent snapshot provided for size estimation"
            echo "104857600"  # 100MB minimum fallback
            return
        fi

        log_info "Estimating incremental backup size..."

        local estimated_size=0
        local parent_path="$SNAP_DIR/$parent_snap"
        local current_path="$SNAP_DIR/$SNAP_NAME"

        # Try the more accurate receive --dump method
        if command -v perl &>/dev/null; then
            estimated_size=$(
                btrfs send --no-data -q -p "$parent_path" "$current_path" 2>/dev/null | \
                btrfs receive --dump 2>/dev/null | \
                grep 'len=' | \
                sed 's/.*len=//' | \
                perl -lne '$sum += $_; END { print $sum || 0 }' 2>/dev/null
            ) || estimated_size=0
        fi

        # If we got a size, add buffer and return. Otherwise use conservative estimate.
        if [[ "$estimated_size" -gt 0 ]]; then
            # Add 30% buffer for metadata overhead and compression variations
            estimated_size=$((estimated_size * 130 / 100))
            log_info "Estimated incremental size: $(format_bytes $estimated_size)"
        else
            # Conservative fallback: 10% of source or 100MB minimum
            local source_size
            source_size=$(du -sb "${config[source_vol]}" 2>/dev/null | cut -f1) || source_size=1073741824
            estimated_size=$((source_size / 10))
            [[ "$estimated_size" -lt 104857600 ]] && estimated_size=104857600
            log_warn "Using conservative estimate: $(format_bytes $estimated_size)"
        fi

        echo "$estimated_size"
    else
        log_info "Calculating full backup size..." >&2
        local btrfs_show_output
        btrfs_show_output=$(btrfs subvolume show "$SNAP_DIR/$SNAP_NAME" 2>/dev/null) || true

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
            size=$(du -sb "$SNAP_DIR/$SNAP_NAME" | cut -f1)
        fi

        echo "$size"
    fi
}

execute_backup_pipeline() {
    local backup_type="$1"
    local parent_snap="${2:-}"  # Optional parent snapshot for incremental

    log_info "Starting $backup_type send with progress monitoring"

    local receive_marker="/tmp/.yabb-receive-$$-$SNAP_NAME"
    local error_log="/tmp/.yabb-error-$$-$SNAP_NAME"

    # Build command array based on backup type
    local -a send_cmd
    if [[ "$backup_type" == "incremental" && -n "$parent_snap" ]]; then
        send_cmd=(
            btrfs send
            -p "$SNAP_DIR/$parent_snap"
            "$SNAP_DIR/$SNAP_NAME"
        )
    else
        send_cmd=(
            btrfs send
            "$SNAP_DIR/$SNAP_NAME"
        )
    fi

    # Execute pipeline
    local pipeline_result=0
    "${send_cmd[@]}" 2>"$error_log.send" | \
        pv -petab 2>"$error_log.pv" | \
        { touch "$receive_marker"; btrfs receive "$DEST_SNAP_DIR/" 2>"$error_log.receive"; } | \
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
        if [[ "$receive_started" == "true" && -d "$DEST_SNAP_DIR/$SNAP_NAME" ]]; then
            log_info "Send failed, removing partial destination snapshot..."
            delete_snapshot "$DEST_SNAP_DIR/$SNAP_NAME" "partial destination" || true
        fi
        rm -f "$error_log".*
        die "btrfs send failed with code ${send_status}"
    fi

    if (( receive_status != 0 )); then
        [[ -s "$error_log.receive" ]] && log_error "Receive error details:" && cat "$error_log.receive" >&2
        if [[ -d "$DEST_SNAP_DIR/$SNAP_NAME" ]]; then
            log_info "Receive failed, removing partial destination snapshot..."
            delete_snapshot "$DEST_SNAP_DIR/$SNAP_NAME" "partial destination" || true
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
    verify_uuids "$SNAP_DIR/$SNAP_NAME" "$DEST_SNAP_DIR/$SNAP_NAME" || {
        die "Destination snapshot UUID mismatch - possible corruption detected\nSource UUID: $SRC_UUID\nDestination UUID: $DEST_UUID"
    }

    BACKUP_SUCCESSFUL=true

    # Perform post-backup device error check
    check_device_errors "${config[dest_mount]}" "post-backup" || {
        log_warn "Device errors detected after backup - backup may be unreliable"
    }

    # Always perform scrub verification
    verify_backup_with_scrub "$DEST_SNAP_DIR/$SNAP_NAME" "destination snapshot" || {
        log_warn "Scrub verification detected issues with backup"
    }

    # Perform checksum sampling if configured
    if [[ "${config[verify_sample_percent]:-0}" -gt 0 ]]; then
        verify_data_checksums "$DEST_SNAP_DIR/$SNAP_NAME" || {
            log_warn "Checksum verification detected issues with backup"
        }
    fi

    log_info "Syncing destination filesystem..."
    btrfs filesystem sync "${config[dest_mount]}"

    if [[ "$VERIFICATION_PASSED" == "true" ]]; then
        log_info "YABB backup successful: ${SNAP_NAME@Q} ($backup_type) - all verifications passed!"
    else
        log_warn "YABB backup completed with warnings: ${SNAP_NAME@Q} ($backup_type) - some verifications failed"
    fi
}

cleanup() {
    local exit_code=$?
    log_info "Performing cleanup..."

    if [[ "$SNAPSHOT_CREATED" == "true" && "$BACKUP_SUCCESSFUL" != "true" ]]; then
        log_warn "Backup failed or was interrupted. Removing snapshots..."

        # Remove source snapshot
        if [[ -d "$SNAP_DIR/$SNAP_NAME" ]]; then
            local retries=3
            while (( retries-- > 0 )); do
                if delete_snapshot "$SNAP_DIR/$SNAP_NAME" "source"; then
                    break
                fi
                (( retries > 0 )) && sleep 1
            done
            (( retries < 0 )) && log_error "Permanent failure removing source snapshot!"
        fi

        # Remove destination snapshot
        if [[ -d "$DEST_SNAP_DIR/$SNAP_NAME" ]]; then
            delete_snapshot "$DEST_SNAP_DIR/$SNAP_NAME" "destination" || \
                log_error "Could not remove destination snapshot!"
        fi
    fi

    # Adjust exit code based on verification status
    if [[ "$exit_code" -eq 0 && "$BACKUP_SUCCESSFUL" == "true" && "$VERIFICATION_PASSED" != "true" ]]; then
        exit_code=2  # Backup succeeded but verification found issues
    fi

    exit $exit_code
}

########################################################
#     Main Process                                      #
########################################################

# Show version/help if requested
if [[ "${1:-}" == "--version" || "${1:-}" == "-v" ]]; then
    echo "YABB (Yet Another BTRFS Backup) v1.4.0"
    exit 0
fi

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
    echo "YABB - Yet Another BTRFS Backup"
    echo ""
    echo "Usage: $(basename "$0") [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --help, -h      Show this help message"
    echo "  --version, -v   Show version information"
    echo "  --restore NAME  Restore snapshot (use 'latest' for most recent)"
    echo ""
    echo "Configuration:"
    echo "  Source: ${config[source_vol]}"
    echo "  Source Snapshots: ${config[source_vol]}/.yabb_snapshots"
    echo "  Destination: ${config[dest_mount]}"
    echo "  Destination Snapshots: ${config[dest_mount]}/.yabb_snapshots"
    echo ""
    echo "Verification Settings:"
    echo "  Post-backup scrub: Always enabled"
    echo "  Device error monitoring: Always enabled"
    echo "  Checksum sampling: ${config[verify_sample_percent]}%"
    echo "  Periodic scrub interval: ${config[minimum_days_between_scrubs]} days"
    exit 0
fi

# Setup error handling
trap 'cleanup' EXIT INT TERM HUP

log_info "YABB (Yet Another BTRFS Backup) starting..."

# Validate environment
check_dependencies
check_mount "source_vol"
check_mount "dest_mount"
ensure_snapshot_directories

# Acquire lock
acquire_lock

# Pre-backup device error check
check_device_errors "${config[source_vol]}" "pre-backup"
check_device_errors "${config[dest_mount]}" "pre-backup"

# Create snapshot
btrfs subvolume snapshot -r "${config[source_vol]}" "$SNAP_DIR/$SNAP_NAME" && \
    SNAPSHOT_CREATED=true || die "Failed to create snapshot ${SNAP_NAME@Q}"
log_info "Created snapshot: ${SNAP_NAME@Q}"

# Verify snapshot
btrfs subvolume show "$SNAP_DIR/$SNAP_NAME" >/dev/null || \
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
    [[ -d "$DEST_SNAP_DIR/$PARENT_SNAP" ]] || \
        die "Parent snapshot ${PARENT_SNAP@Q} missing from destination"

    verify_uuids "$SNAP_DIR/$PARENT_SNAP" "$DEST_SNAP_DIR/$PARENT_SNAP" || \
        die "Parent snapshot UUID mismatch - possible corruption!\nSource UUID: $SRC_UUID\nDest UUID: $DEST_UUID"

    cleanup_partial_snapshot

    # Calculate space needed
    log_info "Checking space requirements for incremental backup..."
    DELTA_SIZE=$(calculate_backup_size "incremental" "$PARENT_SNAP")
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

# Prune old snapshots after successful backup
if [[ "$BACKUP_SUCCESSFUL" == "true" && "${config[retention_days]:-0}" -gt 0 ]]; then
    log_info "Starting snapshot pruning (retention: ${config[retention_days]} days, keep minimum: ${config[keep_minimum]:-5})"
    prune_old_snapshots "$SNAP_DIR"
    prune_old_snapshots "$DEST_SNAP_DIR"
fi

# Schedule periodic scrub if enabled
if [[ "$BACKUP_SUCCESSFUL" == "true" ]]; then
    schedule_periodic_scrub "${config[dest_mount]}"
fi
