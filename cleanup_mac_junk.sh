#!/bin/sh

# cleanup_mac_junk.sh - Clean up macOS-specific metadata files and directories
#
# This script removes various macOS-specific files and directories that are often
# created on network shares and external drives, including:
# - .DS_Store (Desktop Services Store files)
# - ._* files (AppleDouble files containing extended attributes)
# - .AppleDouble directories
# - .AppleDB and .AppleDesktop directories
# - .Trashes directories
#
# Usage examples:
#   ./cleanup_mac_junk.sh              # Clean current directory with confirmation
#   ./cleanup_mac_junk.sh -n           # Dry run (show what would be deleted)
#   ./cleanup_mac_junk.sh -f           # Force delete without confirmation
#   ./cleanup_mac_junk.sh -q           # Quiet mode (minimal output)
#   ./cleanup_mac_junk.sh /some/path   # Clean specific directory
#   ./cleanup_mac_junk.sh -qf /path    # Force quiet clean of specific directory
#
# Options:
#   -h  Show help message
#   -n  Dry run (don't actually delete files)
#   -f  Force deletion (don't ask for confirmation)
#   -q  Quiet mode (minimal output)

# Default values
dry_run=0
force=0
quiet=0
target_dir="."

# Print usage information
usage() {
    cat << EOF
Usage: $(basename "$0") [-h] [-n] [-f] [-q] [directory]
Clean up macOS-specific files and directories recursively.

Options:
    -h  Show this help message
    -n  Dry run (don't actually delete files)
    -f  Force deletion (don't ask for confirmation)
    -q  Quiet mode (minimal output)

If directory is not specified, current directory is used.
EOF
    exit 1
}

# Print error message to stderr
error() {
    printf "Error: %s\n" "$1" >&2
}

# Format size in human readable format
format_size() {
    if [ "$1" -gt 1073741824 ]; then
        echo "$(($1 / 1073741824))G"
    elif [ "$1" -gt 1048576 ]; then
        echo "$(($1 / 1048576))M"
    elif [ "$1" -gt 1024 ]; then
        echo "$(($1 / 1024))K"
    else
        echo "${1}B"
    fi
}

# Process command line options
while getopts "hnfq" opt; do
    case $opt in
        h) usage ;;
        n) dry_run=1 ;;
        f) force=1 ;;
        q) quiet=1 ;;
        ?) usage ;;
    esac
done

# Shift past the options
shift $((OPTIND - 1))

# Check if directory parameter is provided
if [ $# -gt 0 ]; then
    target_dir="$1"
    if [ ! -d "$target_dir" ]; then
        error "Directory '$target_dir' does not exist"
        exit 1
    fi
fi

# Change to target directory
cd "$target_dir" || {
    error "Cannot access directory '$target_dir'"
    exit 1
}

# Initialize counters
total_size=0
total_files=0
total_dirs=0

# First pass: count and display files
if [ $quiet -eq 0 ]; then
    printf "Scanning for macOS-specific files in '%s'...\n" "$target_dir"
fi

find . \( \
    -name ".DS_Store" -o \
    -name "._*" -o \
    -name ".AppleDouble" -o \
    -name ".AppleDB" -o \
    -name ".AppleDesktop" -o \
    -name ".Trashes" \
    \) -print0 | while IFS= read -r -d '' file; do
    if [ -f "$file" ]; then
        size=$(stat -f %z "$file" 2>/dev/null || stat -c %s "$file" 2>/dev/null)
        total_size=$((total_size + size))
        total_files=$((total_files + 1))
        if [ $quiet -eq 0 ]; then
            printf "File: %s (Size: %s)\n" "$file" "$(format_size "$size")"
        fi
    else
        total_dirs=$((total_dirs + 1))
        if [ $quiet -eq 0 ]; then
            printf "Directory: %s\n" "$file"
        fi
    fi
done

# Check if any files were found
if [ $total_files -eq 0 ] && [ $total_dirs -eq 0 ]; then
    if [ $quiet -eq 0 ]; then
        printf "No macOS-specific files found.\n"
    fi
    exit 0
fi

# Show summary
if [ $quiet -eq 0 ]; then
    printf "\nSummary:\n"
    printf "Files: %d\n" "$total_files"
    printf "Directories: %d\n" "$total_dirs"
    printf "Total size: %s\n" "$(format_size "$total_size")"
fi

# Ask for confirmation unless force mode is enabled
if [ $force -eq 0 ] && [ $dry_run -eq 0 ]; then
    printf "\nDo you want to delete these items? (y/N): "
    read -r answer
    case "$answer" in
        [Yy]*) : ;;
        *) 
            printf "Operation cancelled.\n"
            exit 0
            ;;
    esac
fi

# Perform deletion
if [ $dry_run -eq 1 ]; then
    if [ $quiet -eq 0 ]; then
        printf "\nDry run - no files will be deleted.\n"
    fi
else
    if [ $quiet -eq 0 ]; then
        printf "\nDeleting files...\n"
    fi
    find . \( \
        -name ".DS_Store" -o \
        -name "._*" -o \
        -name ".AppleDouble" -o \
        -name ".AppleDB" -o \
        -name ".AppleDesktop" -o \
        -name ".Trashes" \
        \) -print0 | xargs -0 rm -rf 2>/dev/null || {
        error "Some files could not be deleted (permission denied)"
        exit 1
    }
    if [ $quiet -eq 0 ]; then
        printf "Cleanup completed successfully.\n"
    fi
fi
