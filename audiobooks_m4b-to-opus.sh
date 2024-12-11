#!/bin/sh


# Description: Convert audiobooks from m4b to opus
# Detailed notes: https://notes.ameri.coffee/m/jKSdJDVwP9zMkxxUNCnj3Z
# Requirements:
# ffmpeg and GNU Parallel installed on the system
# Note on performance:
# The script parallelises the conversion process and creates
# as many ffmpeg processes as there are cores on the system.

# Enable job control
set -m

# Store script PID and process group
SCRIPT_PID=$$
SCRIPT_PGID=$(ps -o pgid= $$ | tr -d ' ')

# Debug mode (set to 0 to disable debug output)
DEBUG=0

# Check for required commands
for cmd in ffmpeg ffprobe parallel awk grep tr find mktemp; do
    if ! command -v "$cmd" >/dev/null 2>&1; then
        printf "Error: Required command '%s' not found.\n" "$cmd" >&2
        exit 1
    fi
done

debug_print() {
    [ "$DEBUG" = "1" ] && printf "%s\n" "DEBUG: $1" >&2
}

# Cleanup function
cleanup() {
    printf "\n%s\n" "Script interrupted. Cleaning up..."

    debug_print "Starting cleanup process"
    debug_print "Script PID: $SCRIPT_PID, PGID: $SCRIPT_PGID"

    # Kill all processes
    kill -TERM -"$SCRIPT_PGID" 2>/dev/null
    debug_print "Sent TERM signal to process group"

    # Kill specific ffmpeg processes spawned by this script
    pkill -P "$SCRIPT_PID" ffmpeg 2>/dev/null
    debug_print "Sent kill signal to ffmpeg processes"
    sleep 1

    # Force kill any remaining processes
    pkill -KILL -P "$SCRIPT_PID" 2>/dev/null
    kill -KILL -"$SCRIPT_PGID" 2>/dev/null
    debug_print "Sent KILL signal to remaining processes"

    # Remove temporary files
    for tmp in "$counter_total" "$counter_skipped" "$counter_converted" "$tmp_file"; do
        [ -f "$tmp" ] && rm -f "$tmp"
    done

    # Remove partial output files
    find "${1:-.}" -name "*.opus" -size -1M -delete 2>/dev/null

    debug_print "Cleanup completed"
    exit 1
}

# Set up traps
trap cleanup INT TERM QUIT PIPE HUP

# Create temporary files
tmp_file=$(mktemp) || exit 1
counter_total=$(mktemp) || exit 1
counter_skipped=$(mktemp) || exit 1
counter_converted=$(mktemp) || exit 1

# Initialise counters
printf "0\n" > "$counter_total"
printf "0\n" > "$counter_skipped"
printf "0\n" > "$counter_converted"

# Format time in HH:MM:SS
format_time() {
    seconds=$1
    printf "%02d:%02d:%02d" $((seconds/3600)) $((seconds%3600/60)) $((seconds%60))
}

# Check if terminal supports carriage return
check_terminal() {
    if [ -t 1 ]; then
        CR="\r"
    else
        CR="\n"
    fi
}

# Get file size in MB
get_file_size() {
    if [ -f "$1" ]; then
        size=$(ls -l "$1" | awk '{print $5}')
        echo "$((size / 1024 / 1024))"
    else
        echo "0"
    fi
}

# Sanitise file paths
sanitize_path() {
    printf "%s" "$1" | sed 's/[^[:alnum:]._/-]/_/g'
}

# Convert a single file
convert_file() {
    input="$1"
    output="${input%.m4b}.opus"

    # Atomic counter increment
    total=$(($(cat "$counter_total") + 1))
    printf "%d\n" "$total" > "$counter_total"

    printf "[%d/%d] CONVERTING: %s\n" "$total" "$total_found" "$input"
    printf "    Output: %s\n" "$output"

    # Get input duration
    duration=$(ffprobe -v quiet -show_entries format=duration -of default=noprint_wrappers=1:nokey=1 "$input" 2>/dev/null)
    if [ -z "$duration" ] || [ "$duration" = "0" ] || [ "$duration" = "N/A" ]; then
        printf "    Error: Could not determine input file duration\n"
        return 1
    fi

    # Create temporary files for progress monitoring
    progress_file=$(mktemp)
    log_file=$(mktemp)
    start_time=$(date +%s)

    # Start ffmpeg conversion
    ffmpeg -nostdin -threads 1 -i "$input" \
        -c:a libopus \
        -b:a 64k \
        -vbr on \
        -compression_level 10 \
        -frame_duration 60 \
        -application audio \
        -map_metadata 0 \
        -map_chapters 0 \
        -metadata:s:a:0 encoder="libopus" \
        -progress "$progress_file" \
        "$output" 2>"$log_file" &

    ffmpeg_pid=$!
    debug_print "Started ffmpeg process with PID: $ffmpeg_pid"

    # Monitor progress
    while kill -0 $ffmpeg_pid 2>/dev/null; do
        if [ -f "$progress_file" ]; then
            current_time=$(grep "out_time=" "$progress_file" | tail -n1 | cut -d'=' -f2)
            if [ -n "$current_time" ] && [ -n "$duration" ] && [ "$duration" != "0" ]; then
                current_seconds=$(echo "$current_time" | awk -F: '{ print ($1 * 3600) + ($2 * 60) + int($3) }')
                elapsed=$(($(date +%s) - start_time))

                if [ -n "$current_seconds" ] && [ "$current_seconds" != "0" ]; then
                    percent=$(awk -v cur="$current_seconds" -v dur="$duration" 'BEGIN {printf "%.1f", (cur/dur)*100}')

                    if [ -n "$percent" ] && [ "$elapsed" -gt 0 ]; then
                        remaining=$(awk -v e="$elapsed" -v p="$percent" \
                            'BEGIN {p = (p > 0 ? p : 0.1); printf "%d", (e * (100/p)) - e}')

                        elapsed_str=$(format_time $elapsed)
                        remaining_str=$(format_time $remaining)

                        printf "${CR}    Progress: %5.1f%% (Elapsed: %s, Remaining: %s)        " \
                            "$percent" "$elapsed_str" "$remaining_str"
                    else
                        printf "${CR}    Progress: Converting...        "
                    fi
                else
                    printf "${CR}    Progress: Starting conversion...        "
                fi
            else
                printf "${CR}    Progress: Reading file...        "
            fi
        fi
        sleep 1
    done

    wait $ffmpeg_pid
    status=$?
    printf "\n"

    # Cleanup temporary files
    rm -f "$progress_file" "$log_file"

    if [ $status -eq 0 ] && [ -f "$output" ] && [ "$(get_file_size "$output")" -gt 0 ]; then
        printf "    Status: Successfully converted\n"
        converted=$(($(cat "$counter_converted") + 1))
        printf "%d\n" "$converted" > "$counter_converted"
    else
        printf "    Status: Conversion FAILED\n"
        if [ -f "$log_file" ]; then
            printf "    Error log:\n"
            cat "$log_file"
        fi
        [ -f "$output" ] && rm -f "$output"
    fi
    printf "%s\n" "-------------------"
}

# Main script
check_terminal

printf "%s\n" "Starting conversion process..."
printf "Looking for .m4b files in and under: %s\n" "${1:-.}"
printf "%s\n" "-------------------"

# Find all .m4b files without a corresponding .opus file
find "${1:-.}" -type f -name "*.m4b" ! -exec sh -c 'test -f "${1%.m4b}.opus"' sh {} \; -print0 | tr '\0' '\n' > "$tmp_file"

# Check if any files were found
if [ ! -s "$tmp_file" ]; then
    printf "%s\n" "No .m4b files without corresponding .opus found!"
    cleanup
fi

# Print files to be converted
printf "%s\n" "Found the following .m4b files to convert:"
while IFS= read -r file; do
    printf "%s\n" "$file"
done < "$tmp_file"

# Count total files
total_found=$(wc -l < "$tmp_file")
printf "%s\n" "-------------------"
printf "Total files to convert: %d\n" "$total_found"
printf "%s\n" "-------------------"

# Ask for confirmation to proceed
printf "%s" "Proceed with conversion? (y/n): "
read -r choice
case "$choice" in
    [Yy]*)
        : # Continue execution
        ;;
    *)
        printf "%s\n" "Aborting..."
        cleanup
        ;;
esac

# Export necessary variables and functions for parallel
export -f convert_file format_time debug_print get_file_size sanitize_path
export counter_total counter_skipped counter_converted total_found DEBUG CR

# Process files in parallel
parallel --will-cite --line-buffer -j+0 convert_file {} < "$tmp_file"

# Get final counts
total_files=$(cat "$counter_total")
skipped_files=$(cat "$counter_skipped")
converted_files=$(cat "$counter_converted")

# Clean up temporary files
rm -f "$tmp_file" "$counter_total" "$counter_skipped" "$counter_converted"

# Print final statistics
printf "\n%s\n" "Conversion process completed!"
printf "%s\n" "Statistics:"
printf "  Total .m4b files found: %d\n" "$total_found"
printf "  Files processed: %d\n" "$total_files"
printf "  Files skipped: %d\n" "$skipped_files"
printf "  Files converted: %d\n" "$converted_files"
printf "  Failed conversions: %d\n" "$((total_files - skipped_files - converted_files))"