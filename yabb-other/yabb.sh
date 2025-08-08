#!/usr/bin/env bash
# YABB - Yet Another BTRFS Backup
#
export LC_ALL=C
set -euo pipefail
shopt -s lastpipe

########################################################
#     Configuration                                    #
########################################################

declare -A CONFIG=()

validate_integer_range() {
    local value="$1" min="$2" max="$3" name="$4"
    [[ "$value" =~ ^[0-9]+$ ]] && (( value >= min && value <= max )) ||
        die "$name must be an integer between $min-$max (got: '$value')"
}

validate_path() {
    local path="$1" name="$2" must_exist="$3"
    local canonical_path

    if [[ "$must_exist" == "true" ]]; then
        [[ -e "$path" ]] || die "$name path does not exist: $path"
        canonical_path=$(realpath -- "$path" 2>/dev/null) || die "Failed to canonicalize $name: $path"
        [[ "$canonical_path" == /* ]] || die "$name must be an absolute path"
        if [[ "$name" == "YABB_SOURCE_VOL" || "$name" == "YABB_DEST_MOUNT" ]]; then
            [[ -d "$canonical_path" ]] || die "$name must be a directory: $canonical_path"
        fi

        echo "$canonical_path"
    else
        local parent_dir
        parent_dir=$(dirname -- "$path")

        if [[ -d "$parent_dir" ]]; then
            local canonical_parent
            canonical_parent=$(realpath -- "$parent_dir" 2>/dev/null) || die "Failed to canonicalize parent of $name: $parent_dir"
            local basename_part
            basename_part=$(basename -- "$path")
            if [[ "$basename_part" == "." ]] || [[ "$basename_part" == ".." ]] || [[ -z "$basename_part" ]]; then
                die "$name has invalid basename: $basename_part"
            fi
            echo "${canonical_parent}/${basename_part}"
        else
            [[ -n "$path" ]] || die "$name cannot be empty"
            echo "$path"
        fi
    fi
}

validate_rate_limit() {
    local rate="$1" name="$2"
    [[ -z "$rate" ]] && return 0
    # Check format: number followed by unit (K, M, G, or none)
    [[ "$rate" =~ ^[0-9]+[KMG]?$ ]] ||
        die "$name must be a number optionally followed by K, M, or G (got: '$rate')"
}

initialize_config() {
    # Use parameter expansion with :- to handle empty values
    CONFIG[source_vol]="${YABB_SOURCE_VOL:-/data}"
    CONFIG[dest_mount]="${YABB_DEST_MOUNT:-/mnt/external}"
    CONFIG[min_free_gb]="${YABB_MIN_FREE_GB:-1}"
    CONFIG[lock_file]="${YABB_LOCK_FILE:-/var/lock/yabb.lock}"
    CONFIG[retention_days]="${YABB_RETENTION_DAYS:-30}"
    CONFIG[keep_minimum]="${YABB_KEEP_MINIMUM:-5}"
    CONFIG[verify_sample_percent]="${YABB_VERIFY_SAMPLE_PERCENT:-5}"
    CONFIG[minimum_days_between_scrubs]="${YABB_MINIMUM_DAYS_BETWEEN_SCRUBS:-30}"
    CONFIG[scrub_rate_limit]="${YABB_SCRUB_RATE_LIMIT:-}"
}

validate_config() {
    validate_integer_range "${CONFIG[min_free_gb]}" 0 1000000 "YABB_MIN_FREE_GB"  # Up to 1PB
    validate_integer_range "${CONFIG[retention_days]}" 0 36500 "YABB_RETENTION_DAYS"  # Up to 100 years
    validate_integer_range "${CONFIG[keep_minimum]}" 0 100000 "YABB_KEEP_MINIMUM"  # Up to 100k snapshots
    validate_integer_range "${CONFIG[verify_sample_percent]}" 0 100 "YABB_VERIFY_SAMPLE_PERCENT"
    validate_integer_range "${CONFIG[minimum_days_between_scrubs]}" 0 3650 "YABB_MINIMUM_DAYS_BETWEEN_SCRUBS"  # Up to 10 years
    local validated_source
    validated_source=$(validate_path "${CONFIG[source_vol]}" "YABB_SOURCE_VOL" "true") || die "Failed to validate source volume"
    CONFIG[source_vol]="$validated_source"
    local validated_dest
    validated_dest=$(validate_path "${CONFIG[dest_mount]}" "YABB_DEST_MOUNT" "false") || die "Failed to validate destination mount"
    CONFIG[dest_mount]="$validated_dest"
    local validated_lock_dir
    validated_lock_dir=$(validate_path "${CONFIG[lock_file]%/*}" "YABB_LOCK_FILE directory" "false") || die "Failed to validate lock file directory"
    CONFIG[lock_file]="${validated_lock_dir}/$(basename "${CONFIG[lock_file]}")"
    validate_rate_limit "${CONFIG[scrub_rate_limit]}" "YABB_SCRUB_RATE_LIMIT"
    (( CONFIG[keep_minimum] > 0 )) || die "YABB_KEEP_MINIMUM must be at least 1"
    if (( CONFIG[verify_sample_percent] == 0 )); then
        log_warn "Checksum verification is disabled (YABB_VERIFY_SAMPLE_PERCENT=0)"
    fi
}

# For backward compatibility
declare -n config=CONFIG

########################################################
#     Global Variables                                 #
########################################################

SOURCE_BASE=""
SNAP_NAME=""
SRC_UUID=""
DEST_UUID=""
DELTA_SIZE=""
SNAP_DIR=""
DEST_SNAP_DIR=""
PRE_BACKUP_ERRORS=0
POST_BACKUP_ERRORS=0
declare -a TEMP_FILES=()

########################################################
#     State Variables                                  #
########################################################

SNAPSHOT_CREATED=false
BACKUP_SUCCESSFUL=false
VERIFICATION_PASSED=true
RESTORE_OPERATION=false
RESTORE_SUCCESSFUL=false
RESTORE_VERIFICATION_PASSED=true
RECEIVED_SNAPSHOT_PATH=""
RESTORE_POINT_PATH=""

########################################################
#     Function Definitions                              #
########################################################

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

initialize_config
validate_config

initialize_globals() {
    SOURCE_BASE=$(basename -- "${config[source_vol]}")
    if [[ "$SOURCE_BASE" == "/" ]]; then
        SOURCE_BASE="root"
    fi
    SNAP_NAME="${SOURCE_BASE}.$(date -u "+%Y-%m-%dT%H:%M:%SZ")"
    SNAP_DIR="${config[source_vol]}/.yabb_snapshots"
    DEST_SNAP_DIR="${config[dest_mount]}/.yabb_snapshots"
}

initialize_globals

check_dependencies() {
    command -v bc &>/dev/null || die "bc calculator required but not found. Install on Debian with 'sudo apt install bc'"
    command -v pv &>/dev/null || die "pv (Pipe Viewer) required but not found. Install on Debian with 'sudo apt install pv'"
}

verify_uuids() {
    local src_output dest_output src_uuid="" dest_uuid=""
    src_output=$(btrfs subvolume show -- "$1")
    dest_output=$(btrfs subvolume show -- "$2")
    src_uuid=$(grep -i "uuid:" <<< "$src_output" | head -n 1 | awk '{print $2}')
    dest_uuid=$(grep -i "received uuid:" <<< "$dest_output" | awk '{print $3}')
    SRC_UUID="$src_uuid"
    DEST_UUID="$dest_uuid"
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
    [[ $1 =~ ^([0-9.]+)([[:alpha:]]+)?$ ]] || return 1
    local value=${BASH_REMATCH[1]}
    local unit=${BASH_REMATCH[2]^^}
    local factor
    case "$unit" in
        "")    factor=1 ;;          # No unit means bytes
        "B")   factor=1 ;;
        "KB")  factor=1024 ;;
        "MB")  factor=$((1024**2)) ;;
        "GB")  factor=$((1024**3)) ;;
        "TB")  factor=$((1024**4)) ;;
        *)     return 1 ;;          # Invalid unit
    esac
    printf "%.0f\n" "$(echo "$value * $factor" | bc)"
}

# Extract epoch timestamp from snapshot name
get_snapshot_epoch() {
    local snap_name="$1"
    local timestamp="${snap_name#*.}"  # Remove prefix
    date -d -- "${timestamp/T/ }" +%s 2>/dev/null || echo 0
}

check_destination_space() {
    local required_bytes=$1
    local dest_mount="${config[dest_mount]}"
    local buffer=$(convert_to_bytes "${config[min_free_gb]}GB")
    local required_with_buffer=$((required_bytes + buffer))
    local free_bytes=""
    log_info "Checking destination free space on ${dest_mount@Q}..."

    local btrfs_output
    btrfs_output=$(btrfs filesystem usage -b -- "$dest_mount") || die "Failed to check destination filesystem"
    if [[ "$btrfs_output" =~ Free\ \(estimated\):[[:space:]]+([0-9]+) ]]; then
        free_bytes=${BASH_REMATCH[1]}
    else
        die "Could not parse free space from btrfs output"
    fi
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

release_lock() {
    if { >&9; } 2>/dev/null; then
        exec 9>&-
    fi
    if [[ -f "${config[lock_file]}" ]]; then
        local lock_pid=$(cat -- "${config[lock_file]}" 2>/dev/null || echo 0)
        if [[ "$lock_pid" == "$$" ]]; then
            rm -f -- "${config[lock_file]}"
        fi
    fi
}

register_temp_file() {
    local temp_file="$1"
    if [[ -n "$temp_file" ]]; then
        TEMP_FILES+=("$temp_file")
    fi
}

cleanup_temp_files() {
    local temp_file
    for temp_file in "${TEMP_FILES[@]}"; do
        if [[ -f "$temp_file" ]]; then
            rm -f -- "$temp_file" 2>/dev/null || true
        fi
    done
    TEMP_FILES=()
}

ensure_snapshot_directories() {
    if [[ ! -d "$SNAP_DIR" ]]; then
        log_info "Creating source snapshot directory: ${SNAP_DIR@Q}"
        mkdir -p -- "$SNAP_DIR" || die "Failed to create snapshot directory ${SNAP_DIR@Q}"
    fi

    if [[ ! -d "$DEST_SNAP_DIR" ]]; then
        log_info "Creating destination snapshot directory: ${DEST_SNAP_DIR@Q}"
        mkdir -p -- "$DEST_SNAP_DIR" || die "Failed to create destination snapshot directory ${DEST_SNAP_DIR@Q}"
    fi
}

check_mount() {
    local config_key="$1"
    local mount_path="${config[$config_key]}"
    mountpoint -q -- "$mount_path" && return 0
    log_info "Attempting to mount ${mount_path@Q}..."
    mount -- "$mount_path" || die "Failed to mount ${mount_path@Q}"
    mountpoint -q -- "$mount_path" || die "Mount verification failed for ${mount_path@Q}"
}

find_parent_snapshot() {
    local -a snapshots
    find -- "$SNAP_DIR" -maxdepth 1 -name "${SOURCE_BASE}.*" -printf '%T@ %p\0' |
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
    # Normal delete first
    if btrfs subvolume delete -- "$snapshot_path" 2>/dev/null; then
        log_info "Successfully removed $description snapshot"
        return 0
    fi
    # Try with -c flag for partial/corrupted snapshots
    if btrfs subvolume delete -c -- "$snapshot_path" 2>/dev/null; then
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

check_device_errors() {
    local mount_point="$1"
    local phase="$2"
    log_info "Checking device error statistics ($phase)..."
    local stats_output exit_code
    stats_output=$(btrfs device stats --check -- "$mount_point" 2>&1)
    exit_code=$?

    if [[ $exit_code -ne 0 ]]; then
        log_warn "Device errors detected on $mount_point:"
        btrfs device stats -- "$mount_point" 2>&1 | grep -v " 0$" >&2

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

verify_backup_with_scrub() {
    local snapshot_path="$1"
    local description="$2"
    log_info "Starting integrity verification via scrub for $description..."
    local -a scrub_cmd=(btrfs scrub start -B)
    [[ -n "${config[scrub_rate_limit]}" ]] && scrub_cmd+=(-r "${config[scrub_rate_limit]}")
    scrub_cmd+=(-- "$snapshot_path")
    local scrub_output
    if scrub_output=$("${scrub_cmd[@]}" 2>&1); then
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

verify_data_checksums() {
    local snapshot="$1"
    local sample_percentage="${2:-${config[verify_sample_percent]:-5}}"  # Default 5% sampling
    # Skip if sampling is disabled
    [[ "$sample_percentage" -eq 0 ]] && return 0
    log_info "Verifying data integrity via checksum reads (${sample_percentage}% sample)..."
    local total_files verified_files=0 failed_files=0
    total_files=$(find -- "$snapshot" -type f 2>/dev/null | wc -l)
    [[ "$total_files" -eq 0 ]] && { log_info "No files to verify in snapshot"; return 0; }
    local sample_size=$((total_files * sample_percentage / 100))
    [[ $sample_size -lt 10 && $total_files -ge 10 ]] && sample_size=10  # Minimum 10 files if available
    [[ $sample_size -gt $total_files ]] && sample_size=$total_files
    log_info "Sampling $sample_size of $total_files files for checksum verification..."
    # Build array of files using null-delimited input to handle filenames with newlines
    local -a files=()
    local file
    while IFS= read -r -d '' file; do
        files+=("$file")
    done < <(find -- "$snapshot" -type f -print0 2>/dev/null)
    
    # Fisher-Yates shuffle to randomly sample files
    local -a sampled_files=()
    local num_files=${#files[@]}
    if [[ $num_files -gt 0 ]]; then
        # Take either sample_size or all files, whichever is smaller
        local files_to_sample=$sample_size
        [[ $files_to_sample -gt $num_files ]] && files_to_sample=$num_files
        
        # If we need all files, just copy the array
        if [[ $files_to_sample -eq $num_files ]]; then
            sampled_files=("${files[@]}")
        else
            # Fisher-Yates shuffle for random sampling
            local -a indices=()
            for ((i=0; i<num_files; i++)); do
                indices[i]=$i
            done
            for ((i=0; i<files_to_sample; i++)); do
                local j=$((i + RANDOM % (num_files - i)))
                local temp=${indices[i]}
                indices[i]=${indices[j]}
                indices[j]=$temp
                sampled_files+=("${files[${indices[i]}]}")
            done
        fi
    fi
    
    # Verify the sampled files
    local dd_error
    for file in "${sampled_files[@]}"; do
        ((verified_files++))
        if ! dd_error=$(dd if="$file" of=/dev/null bs=1M status=none 2>&1); then
            ((failed_files++))
            log_error "Checksum verification failed for: $file"
            [[ -n "$dd_error" ]] && log_error "  Error details: $dd_error"
        fi
        # Progress indication every 100 files
        if (( verified_files % 100 == 0 )); then
            log_info "Verified $verified_files/$sample_size files..."
        fi
    done
    if [[ $failed_files -gt 0 ]]; then
        log_error "Checksum verification failed for $failed_files files"
        VERIFICATION_PASSED=false
        return 1
    else
        log_info "Successfully verified $verified_files files (no checksum errors)"
        return 0
    fi
}

find_restore_snapshot() {
    local snapshot_name="${1:-latest}"
    local search_dir="${DEST_SNAP_DIR}"
    if [[ "$snapshot_name" == "latest" ]]; then
        # Find most recent snapshot
        local latest_snap
        latest_snap=$(find -- "$search_dir" -maxdepth 1 -name "${SOURCE_BASE}.*" \
                     -printf '%T@ %p\n' 2>/dev/null | \
                     sort -rn | head -n 1 | cut -d' ' -f2)
        [[ -z "$latest_snap" ]] && return 1
        echo "$latest_snap"
    else
        # Check if specific snapshot exists
        local specific_snap="$search_dir/$snapshot_name"
        [[ -d "$specific_snap" ]] || return 1
        echo "$specific_snap"
    fi
}

calculate_restore_space() {
    local source_snapshot="$1"
    local snap_size
    snap_size=$(du -sb -- "$source_snapshot" 2>/dev/null | cut -f1) || {
        # Fallback to a conservative estimate
        echo "1073741824"  # 1GB minimum
        return
    }
    # Add 20% buffer for filesystem overhead
    echo $((snap_size * 120 / 100))
}

check_restore_destination_space() {
    local required_bytes=$1
    local restore_parent_dir="${2:-$(dirname -- "${config[source_vol]}")}"
    local buffer=$(convert_to_bytes "${config[min_free_gb]}GB")
    local required_with_buffer=$((required_bytes + buffer))
    log_info "Checking restore destination free space..."
    # Check if restore parent is on BTRFS
    local fs_type
    fs_type=$(stat -f -c %T -- "$restore_parent_dir" 2>/dev/null) || \
        fs_type=$(df -T -- "$restore_parent_dir" | tail -n 1 | awk '{print $2}')

    if [[ "$fs_type" != "btrfs" ]]; then
        die "Restore destination must be on a BTRFS filesystem (found: $fs_type)"
    fi
    # Get free space
    local btrfs_output free_bytes
    btrfs_output=$(btrfs filesystem usage -b -- "$restore_parent_dir") || \
        die "Failed to check restore destination filesystem"

    if [[ "$btrfs_output" =~ Free\ \(estimated\):[[:space:]]+([0-9]+) ]]; then
        free_bytes=${BASH_REMATCH[1]}
    else
        die "Could not parse free space from btrfs output"
    fi
    log_info "Restore destination space status:"
    log_info " - Available: $(format_bytes "$free_bytes")"
    log_info " - Required: $(format_bytes "$required_with_buffer") (including ${config[min_free_gb]}GB buffer)"
    ((free_bytes < required_with_buffer)) && \
        die "Insufficient space for restore (needs $(format_bytes $required_with_buffer))"
    log_info "Space check passed for restore operation"
}

execute_restore_pipeline() {
    local source_snapshot="$1"
    local destination_parent="$2"
    local snapshot_basename=$(basename -- "$source_snapshot")
    log_info "Starting restore send/receive pipeline..." >&2
    local receive_marker=$(mktemp /tmp/.yabb-restore-receive.XXXXXX) || die "Failed to create temporary marker file"
    local error_log_base=$(mktemp -u /tmp/.yabb-restore-error.XXXXXX) || die "Failed to create temporary error log base"
    local error_log="${error_log_base}"
    register_temp_file "$receive_marker"
    register_temp_file "${error_log_base}.send"
    register_temp_file "${error_log_base}.receive"
    set +o pipefail
    btrfs send -- "$source_snapshot" 2>"$error_log.send" | \
        pv -petab 2>"$error_log.pv" | \
        { touch -- "$receive_marker"; btrfs receive -- "$destination_parent/" 2>"$error_log.receive"; }
    local -a pipeline_status=("${PIPESTATUS[@]}")
    set -o pipefail
    local send_status=${pipeline_status[0]:-0}
    local pv_status=${pipeline_status[1]:-0}
    local receive_status=${pipeline_status[2]:-0}

    local receive_started=false
    [[ -f "$receive_marker" ]] && receive_started=true
    rm -f -- "$receive_marker"
    if (( send_status != 0 )); then
        [[ -s "$error_log.send" ]] && log_error "Send error:" && cat -- "$error_log.send" >&2

        if [[ "$receive_started" == "true" ]]; then
            local partial_path="$destination_parent/$snapshot_basename"
            if [[ -d "$partial_path" ]]; then
                log_info "Send failed after receive started, removing partial restore..."
                delete_snapshot "$partial_path" "partial restore" || true
            fi
        fi
        rm -f -- "$error_log".*
        die "btrfs send failed during restore (code: $send_status)"
    fi

    if (( receive_status != 0 )); then
        [[ -s "$error_log.receive" ]] && log_error "Receive error:" && cat -- "$error_log.receive" >&2

        local partial_path="$destination_parent/$snapshot_basename"
        if [[ -d "$partial_path" ]]; then
            log_info "Receive failed, removing partial restore..."
            delete_snapshot "$partial_path" "partial restore" || true
        fi
        rm -f -- "$error_log".*
        die "btrfs receive failed during restore (code: $receive_status)"
    fi

    if (( pv_status != 0 && pv_status != 141 )); then
        # Exit code 141 is SIGPIPE which can happen normally
        [[ -s "$error_log.pv" ]] && log_warn "Progress monitor warning:" && cat -- "$error_log.pv" >&2
        # Don't die on pv errors if send and receive succeeded
        if (( pv_status > 1 && pv_status != 141 )); then
            log_warn "Progress monitor (pv) had issues but restore continued (code ${pv_status})"
        fi
    fi

    local restored_path="$destination_parent/$snapshot_basename"
    if [[ ! -d "$restored_path" ]]; then
        rm -f -- "$error_log".*
        die "Restore pipeline completed but snapshot was not created at $restored_path"
    fi
    rm -f -- "$error_log".*
    log_info "Restore pipeline completed successfully" >&2
    # Return the path of received snapshot
    echo "$restored_path"
}

restore_from_backup() {
    local snapshot_name="${1:-latest}"
    local custom_restore_point="${2:-}"
    RESTORE_OPERATION=true
    log_info "=== Starting YABB Emergency Recovery Mode ==="
    log_info "Restore request: ${snapshot_name@Q}"
    check_dependencies
    check_mount "dest_mount"
    acquire_lock

    local source_snapshot
    source_snapshot=$(find_restore_snapshot "$snapshot_name") || \
        die "Cannot find snapshot: ${snapshot_name@Q}"
    local snapshot_basename=$(basename -- "$source_snapshot")
    log_info "Found snapshot to restore: $snapshot_basename"

    log_info "Performing pre-restore integrity verification..."
    check_device_errors "${config[dest_mount]}" "pre-restore"
    verify_backup_with_scrub "$source_snapshot" "source snapshot" || {
        log_error "Source snapshot failed integrity check - aborting restore"
        RESTORE_VERIFICATION_PASSED=false
        die "Cannot restore from corrupted snapshot"
    }

    if [[ "${config[verify_sample_percent]:-5}" -gt 0 ]]; then
        verify_data_checksums "$source_snapshot" || {
            log_error "Checksum verification failed - aborting restore"
            RESTORE_VERIFICATION_PASSED=false
            die "Cannot restore from snapshot with checksum errors"
        }
    fi

    log_info "Pre-restore verification passed"
    local restore_parent_dir=$(dirname -- "${config[source_vol]}")
    local restore_point
    if [[ -n "$custom_restore_point" ]]; then
        restore_point="$custom_restore_point"
        restore_parent_dir=$(dirname -- "$restore_point")
    else
        # Default: create .restore version next to original
        restore_point="${config[source_vol]}.restore"
    fi

    if [[ -d "$restore_point" ]]; then
        die "Restore destination already exists: ${restore_point@Q}"
    fi

    local required_space
    required_space=$(calculate_restore_space "$source_snapshot")
    check_restore_destination_space "$required_space" "$restore_parent_dir"
    log_info "Restoring snapshot to: ${restore_point@Q}"

    local received_path
    received_path=$(execute_restore_pipeline "$source_snapshot" "$restore_parent_dir")
    RECEIVED_SNAPSHOT_PATH="$received_path"

    log_info "Snapshot received at: $received_path"
    log_info "Creating read-write restore point..."
    btrfs subvolume snapshot -- "$received_path" "$restore_point" || {
        delete_snapshot "$received_path" "received" || true
        die "Failed to create read-write restore snapshot"
    }
    RESTORE_POINT_PATH="$restore_point"  # Track for cleanup if needed

    delete_snapshot "$received_path" "temporary read-only" || \
        log_warn "Could not remove temporary read-only snapshot: $received_path"

    log_info "Performing post-restore verification..."
    verify_data_checksums "$restore_point" 10 || {
        log_error "Post-restore verification failed!"
        RESTORE_VERIFICATION_PASSED=false
        # Don't die here - data is restored but may have issues
    }

    check_device_errors "$restore_parent_dir" "post-restore"

    RESTORE_SUCCESSFUL=true
    log_info "Syncing filesystem..."
    btrfs filesystem sync -- "$restore_parent_dir"
    log_info "========================================="
    if [[ "$RESTORE_VERIFICATION_PASSED" == "true" ]]; then
        log_info "RESTORE COMPLETED SUCCESSFULLY"
    else
        log_warn "RESTORE COMPLETED WITH VERIFICATION WARNINGS"
    fi
    log_info "========================================="
    log_info "Restored data location: ${restore_point@Q}"
}

schedule_periodic_scrub() {
    local mount_point="$1"
    local min_days_between_scrubs="${config[minimum_days_between_scrubs]:-30}"
    # Skip if periodic scrub is disabled (0 means disabled)
    [[ "$min_days_between_scrubs" -eq 0 ]] && return 0

    local state_dir="/var/lib/yabb"
    [[ -d "$state_dir" ]] || mkdir -p -- "$state_dir" 2>/dev/null || state_dir="/tmp"
    local mount_point_safe="${mount_point//\//_}"
    mount_point_safe="${mount_point_safe//[^a-zA-Z0-9_-]/_}"
    local last_scrub_file="$state_dir/.last_scrub_${mount_point_safe}"

    local current_time=$(date -- +%s)
    local last_scrub_time=0

    [[ -f "$last_scrub_file" ]] && last_scrub_time=$(cat -- "$last_scrub_file" 2>/dev/null || echo 0)
    local days_since_scrub=$(( (current_time - last_scrub_time) / 86400 ))
    if [[ $days_since_scrub -gt $min_days_between_scrubs ]]; then
        log_info "Starting periodic scrub (last scrub: ${days_since_scrub} days ago, threshold: ${min_days_between_scrubs} days)..."

        local -a scrub_cmd=(btrfs scrub start -B)
        [[ -n "${config[scrub_rate_limit]}" ]] && scrub_cmd+=(-r "${config[scrub_rate_limit]}")
        scrub_cmd+=(-- "$mount_point")

        if "${scrub_cmd[@]}" 2>&1; then
            local temp_scrub_file=$(mktemp "$state_dir/.last_scrub_${mount_point_safe}.XXXXXX") || {
                log_warn "Failed to create temp file for scrub timestamp"
                return 1
            }
            register_temp_file "$temp_scrub_file"
            echo "$current_time" > "$temp_scrub_file" && \
                mv -f -- "$temp_scrub_file" "$last_scrub_file" || {
                    rm -f -- "$temp_scrub_file"
                    log_warn "Failed to update scrub timestamp"
                }
            log_info "Periodic scrub completed successfully"
        else
            log_warn "Periodic scrub encountered errors"
        fi
    fi
}

prune_old_snapshots() {
    local location="$1"  # Either snap_dir or dest_mount

    # Skip if retention disabled
    [[ "${config[retention_days]:-0}" -eq 0 ]] && return 0
    [[ ! -d "$location" ]] && return 1

    local cutoff_epoch=$(($(date -- +%s) - config[retention_days] * 86400))
    local -a snapshots_to_delete=()
    local -a all_snapshots=()

    while IFS= read -r -d '' snapshot; do
        local snap_name=$(basename -- "$snapshot")
        [[ "$snap_name" == "$SNAP_NAME" ]] && continue

        local snap_epoch=$(get_snapshot_epoch "$snap_name")
        [[ "$snap_epoch" -eq 0 ]] && continue  # Skip if can't parse date

        all_snapshots+=("$snap_epoch:$snapshot")
    done < <(find -- "$location" -maxdepth 1 -name "${SOURCE_BASE}.*" -type d -print0)

    # Sort by age (oldest first)
    # Use a subshell to protect IFS
    local sorted_snapshots_raw
    sorted_snapshots_raw=$(printf '%s\n' "${all_snapshots[@]}" | sort -n) || return 1
    local -a sorted_snapshots=()
    if [[ -n "$sorted_snapshots_raw" ]]; then
        mapfile -t sorted_snapshots <<< "$sorted_snapshots_raw"
    fi
    local total_count=${#sorted_snapshots[@]}
    local keep_count="${config[keep_minimum]:-5}"
    for snapshot_info in "${sorted_snapshots[@]}"; do
        local epoch="${snapshot_info%%:*}"
        local path="${snapshot_info#*:}"

        # Keep minimum number regardless of age
        if (( total_count - ${#snapshots_to_delete[@]} <= keep_count )); then
            break
        fi

        if (( epoch < cutoff_epoch )); then
            snapshots_to_delete+=("$path")
        fi
    done

    if [[ ${#snapshots_to_delete[@]} -gt 0 ]]; then
        log_info "Pruning ${#snapshots_to_delete[@]} old snapshots from $location"

        for snapshot in "${snapshots_to_delete[@]}"; do
            delete_snapshot "$snapshot" "old" || \
                log_warn "Failed to prune: $(basename -- "$snapshot")"
        done
    fi
}

calculate_backup_size() {
    local backup_type="$1"
    if [[ "$backup_type" == "incremental" ]]; then
        local parent_snap="${2:-}"

        if [[ -z "$parent_snap" ]]; then
            log_warn "No parent snapshot provided for size estimation"
            echo "104857600"  # 100MB minimum fallback
            return
        fi
        log_info "Estimating incremental backup size..." >&2
        local estimated_size=0
        local parent_path="$SNAP_DIR/$parent_snap"
        local current_path="$SNAP_DIR/$SNAP_NAME"
        if command -v perl &>/dev/null; then
            estimated_size=$(
                btrfs send --no-data -q -p "$parent_path" -- "$current_path" 2>/dev/null | \
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
            log_info "Estimated incremental size: $(format_bytes $estimated_size)" >&2
        else
            # Conservative fallback: 10% of source or 100MB minimum
            local source_size
            source_size=$(du -sb -- "${config[source_vol]}" 2>/dev/null | cut -f1) || source_size=1073741824
            estimated_size=$((source_size / 10))
            [[ "$estimated_size" -lt 104857600 ]] && estimated_size=104857600
            log_warn "Using conservative estimate: $(format_bytes $estimated_size)"
        fi
        echo "$estimated_size"
    else
        log_info "Calculating full backup size..." >&2
        local btrfs_show_output
        btrfs_show_output=$(btrfs subvolume show -- "$SNAP_DIR/$SNAP_NAME" 2>/dev/null) || true
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
            size=$(du -sb -- "$SNAP_DIR/$SNAP_NAME" | cut -f1)
        fi

        echo "$size"
    fi
}

execute_backup_pipeline() {
    local backup_type="$1"
    local parent_snap="${2:-}"  # Optional parent snapshot for incremental
    log_info "Starting $backup_type send with progress monitoring"
    local receive_marker=$(mktemp /tmp/.yabb-receive.XXXXXX) || die "Failed to create temporary marker file"
    local error_log_base=$(mktemp -u /tmp/.yabb-error.XXXXXX) || die "Failed to create temporary error log base"
    local error_log="${error_log_base}"
    register_temp_file "$receive_marker"
    register_temp_file "${error_log_base}.send"
    register_temp_file "${error_log_base}.receive"

    # Build command array based on backup type
    local -a send_cmd
    if [[ "$backup_type" == "incremental" && -n "$parent_snap" ]]; then
        send_cmd=(
            btrfs send
            -p "$SNAP_DIR/$parent_snap"
            -- "$SNAP_DIR/$SNAP_NAME"
        )
    else
        send_cmd=(
            btrfs send
            -- "$SNAP_DIR/$SNAP_NAME"
        )
    fi

    set +o pipefail
    "${send_cmd[@]}" 2>"$error_log.send" | \
        pv -petab 2>"$error_log.pv" | \
        { touch -- "$receive_marker"; btrfs receive -- "$DEST_SNAP_DIR/" 2>"$error_log.receive"; }

    local -a pipeline_status=("${PIPESTATUS[@]}")
    set -o pipefail

    local send_status=${pipeline_status[0]:-0}
    local pv_status=${pipeline_status[1]:-0}
    local receive_status=${pipeline_status[2]:-0}
    local receive_started=false
    [[ -f "$receive_marker" ]] && receive_started=true
    rm -f -- "$receive_marker"

    if (( send_status != 0 )); then
        [[ -s "$error_log.send" ]] && log_error "Send error details:" && cat -- "$error_log.send" >&2

        # Clean up partial snapshot if receive had started
        if [[ "$receive_started" == "true" && -d "$DEST_SNAP_DIR/$SNAP_NAME" ]]; then
            log_info "Send failed after receive started, removing partial destination snapshot..."
            delete_snapshot "$DEST_SNAP_DIR/$SNAP_NAME" "partial destination" || true
        fi
        rm -f -- "$error_log".*
        die "btrfs send failed with code ${send_status}"
    fi

    if (( receive_status != 0 )); then
        [[ -s "$error_log.receive" ]] && log_error "Receive error details:" && cat -- "$error_log.receive" >&2
        # Clean up partial receive
        if [[ -d "$DEST_SNAP_DIR/$SNAP_NAME" ]]; then
            log_info "Receive failed, removing partial destination snapshot..."
            delete_snapshot "$DEST_SNAP_DIR/$SNAP_NAME" "partial destination" || true
        fi
        rm -f -- "$error_log".*
        die "btrfs receive failed with code ${receive_status}"
    fi

    if (( pv_status != 0 && pv_status != 141 )); then
        # Exit code 141 is SIGPIPE which can happen normally when grep filters output
        [[ -s "$error_log.pv" ]] && log_warn "Progress monitor warning:" && cat -- "$error_log.pv" >&2
        # Don't die on pv errors if send and receive succeeded
        if (( pv_status > 1 && pv_status != 141 )); then
            log_warn "Progress monitor (pv) had issues but backup continued (code ${pv_status})"
        fi
    fi

    if [[ ! -d "$DEST_SNAP_DIR/$SNAP_NAME" ]]; then
        rm -f -- "$error_log".*
        die "Backup pipeline completed but destination snapshot was not created"
    fi

    rm -f -- "$error_log".*
    log_info "Backup pipeline completed successfully"
}

finalize_backup() {
    local backup_type="$1"
    log_info "Verifying destination snapshot integrity..."
    verify_uuids "$SNAP_DIR/$SNAP_NAME" "$DEST_SNAP_DIR/$SNAP_NAME" || {
        die "Destination snapshot UUID mismatch - possible corruption detected\nSource UUID: $SRC_UUID\nDestination UUID: $DEST_UUID"
    }

    BACKUP_SUCCESSFUL=true
    check_device_errors "${config[dest_mount]}" "post-backup" || {
        log_warn "Device errors detected after backup - backup may be unreliable"
    }

    verify_backup_with_scrub "$DEST_SNAP_DIR/$SNAP_NAME" "destination snapshot" || {
        log_warn "Scrub verification detected issues with backup"
    }

    if [[ "${config[verify_sample_percent]:-0}" -gt 0 ]]; then
        verify_data_checksums "$DEST_SNAP_DIR/$SNAP_NAME" || {
            log_warn "Checksum verification detected issues with backup"
        }
    fi
    log_info "Syncing destination filesystem..."
    btrfs filesystem sync -- "${config[dest_mount]}"
    if [[ "$VERIFICATION_PASSED" == "true" ]]; then
        log_info "YABB backup successful: ${SNAP_NAME@Q} ($backup_type) - all verifications passed!"
    else
        log_warn "YABB backup completed with warnings: ${SNAP_NAME@Q} ($backup_type) - some verifications failed"
    fi
}

cleanup() {
    local exit_code=$?
    log_info "Performing cleanup..."
    release_lock
    cleanup_temp_files

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

    if [[ "$exit_code" -eq 0 && "$BACKUP_SUCCESSFUL" == "true" && "$VERIFICATION_PASSED" != "true" ]]; then
        exit_code=2  # Backup succeeded but verification found issues
    fi

    exit $exit_code
}

cleanup_restore() {
    local exit_code=$?

    if [[ "$RESTORE_OPERATION" != "true" ]]; then
        return  # Not a restore operation
    fi
    log_info "Performing restore cleanup..."
    release_lock
    cleanup_temp_files
    # Clean up failed restore attempts
    if [[ "$RESTORE_SUCCESSFUL" != "true" ]]; then
        log_warn "Restore operation failed or was interrupted"
        # Remove partial received snapshot if exists
        if [[ -n "$RECEIVED_SNAPSHOT_PATH" && -d "$RECEIVED_SNAPSHOT_PATH" ]]; then
            delete_snapshot "$RECEIVED_SNAPSHOT_PATH" "partial restore" || \
                log_error "Could not remove partial restore snapshot"
        fi
        # Remove restore point if exists
        if [[ -n "$RESTORE_POINT_PATH" && -d "$RESTORE_POINT_PATH" ]]; then
            delete_snapshot "$RESTORE_POINT_PATH" "restore point" || \
                log_error "Could not remove restore point snapshot"
        fi
    fi
    if [[ "$exit_code" -eq 0 && "$RESTORE_SUCCESSFUL" == "true" && \
          "$RESTORE_VERIFICATION_PASSED" != "true" ]]; then
        exit_code=3  # Restore succeeded but verification found issues
    fi

    exit $exit_code
}

########################################################
#     Main Process                                      #
########################################################

if [[ "${1:-}" == "--version" || "${1:-}" == "-v" ]]; then
    echo "YABB (Yet Another BTRFS Backup) v0.1.0"
    exit 0
fi

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
    echo "YABB - Yet Another BTRFS Backup"
    echo ""
    echo "Usage: $(basename -- "$0") [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --help, -h      Show this help message"
    echo "  --version, -v   Show version information"
    echo "  --restore NAME [PATH]  Restore snapshot to optional PATH"
    echo "                         NAME: snapshot name or 'latest' for most recent"
    echo "                         PATH: custom restore location (default: SOURCE.restore)"
    echo ""
    echo "Environment Variables:"
    echo "  YABB_SOURCE_VOL                    Source volume path (default: /data)"
    echo "  YABB_DEST_MOUNT                    Destination mount point (default: /mnt/external)"
    echo "  YABB_MIN_FREE_GB                   Minimum free space in GB (default: 1)"
    echo "  YABB_LOCK_FILE                     Lock file path (default: /var/lock/yabb.lock)"
    echo "  YABB_RETENTION_DAYS                Days to retain snapshots (default: 30, 0=disabled)"
    echo "  YABB_KEEP_MINIMUM                  Minimum snapshots to keep (default: 5)"
    echo "  YABB_VERIFY_SAMPLE_PERCENT         File sample % for verification (default: 5, 0=disabled)"
    echo "  YABB_MINIMUM_DAYS_BETWEEN_SCRUBS   Days between scrubs (default: 30, 0=disabled)"
    echo "  YABB_SCRUB_RATE_LIMIT              Scrub rate limit, e.g., 100M (default: none)"
    echo ""
    echo "Current Configuration:"
    echo "  Source: ${config[source_vol]}"
    echo "  Destination: ${config[dest_mount]}"
    echo "  Min Free GB: ${config[min_free_gb]}"
    echo "  Retention Days: ${config[retention_days]}"
    echo "  Keep Minimum: ${config[keep_minimum]}"
    echo "  Verify Sample: ${config[verify_sample_percent]}%"
    echo "  Scrub Interval: ${config[minimum_days_between_scrubs]} days"
    echo "  Scrub Rate Limit: ${config[scrub_rate_limit]:-none}"
    echo ""
    echo "Examples:"
    echo "  # Use custom paths"
    echo "  YABB_SOURCE_VOL=/home YABB_DEST_MOUNT=/backup ./yabb.sh"
    echo ""
    echo "  # Disable verification"
    echo "  YABB_VERIFY_SAMPLE_PERCENT=0 ./yabb.sh"
    echo ""
    echo "  # Restore latest snapshot"
    echo "  ./yabb.sh --restore latest"
    echo ""
    echo "  # Restore specific snapshot to custom location"
    echo "  ./yabb.sh --restore data.2024-01-15T10:30:00Z /mnt/recovery/data"
    exit 0
fi

if [[ "${1:-}" == "--restore" ]]; then
    shift  # Remove --restore from arguments
    # Override trap for restore-specific cleanup
    trap 'cleanup_restore' EXIT INT TERM HUP

    restore_from_backup "${1:-latest}" "${2:-}"

    if [[ "$RESTORE_SUCCESSFUL" == "true" ]]; then
        if [[ "$RESTORE_VERIFICATION_PASSED" == "true" ]]; then
            exit 0
        else
            exit 3  # Success with warnings
        fi
    else
        exit 1  # Failure
    fi
fi

trap 'cleanup' EXIT INT TERM HUP
log_info "YABB (Yet Another BTRFS Backup) starting..."
# Validate environment
check_dependencies
check_mount "source_vol"
check_mount "dest_mount"
ensure_snapshot_directories
# Acquire lock
acquire_lock
# Device error check
check_device_errors "${config[source_vol]}" "pre-backup"
check_device_errors "${config[dest_mount]}" "pre-backup"
# Create snapshot
btrfs subvolume snapshot -r -- "${config[source_vol]}" "$SNAP_DIR/$SNAP_NAME" && \
    SNAPSHOT_CREATED=true || die "Failed to create snapshot ${SNAP_NAME@Q}"
log_info "Created snapshot: ${SNAP_NAME@Q}"
# Verify snapshot
btrfs subvolume show -- "$SNAP_DIR/$SNAP_NAME" >/dev/null || \
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
    log_info "Checking space requirements for incremental backup..."
    DELTA_SIZE=$(calculate_backup_size "incremental" "$PARENT_SNAP")
    check_destination_space "$DELTA_SIZE" || {
        BACKUP_SUCCESSFUL=false
        die "Aborting backup due to insufficient space"
    }

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

# Prune old snapshots
if [[ "$BACKUP_SUCCESSFUL" == "true" && "${config[retention_days]:-0}" -gt 0 ]]; then
    log_info "Starting snapshot pruning (retention: ${config[retention_days]} days, keep minimum: ${config[keep_minimum]:-5})"
    prune_old_snapshots "$SNAP_DIR"
    prune_old_snapshots "$DEST_SNAP_DIR"
fi

# Periodic scrub
if [[ "$BACKUP_SUCCESSFUL" == "true" ]]; then
    schedule_periodic_scrub "${config[dest_mount]}"
fi
