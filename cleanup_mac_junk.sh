#!/bin/sh

# Description: https://notes.ameri.coffee/m/e4eonVD7jfdwPUtLMtSjgC

# cleanup_mac_junk.sh - Clean up macOS-specific metadata junk files
#
# This script removes various macOS-specific files and directories that are often
# created on network shares and external storage, including:
# - .DS_Store (Desktop Services Store files)
# - ._* files (AppleDouble files containing extended attributes)
# - .AppleDouble directories
# - .AppleDB and .AppleDesktop directories
# - .Trashes directories

# Default values
dry_run=0
force=0
quiet=0
target_dir="."
exclude_dirs=""

# Print error message to stderr
error() {
    printf "Error: %s\n" "$1" >&2
}

# Print usage information
usage() {
    cat << EOF
Usage: $(basename "$0") [-h] [-n] [-f] [-q] [--exclude dir] [directory]
Clean up macOS-specific files and directories recursively.

Options:
    -h, --help     Show this help message
    -n, --dry-run  Dry run (don't actually delete files)
    -f, --force    Force deletion (don't ask for confirmation)
    -q, --quiet    Quiet mode (minimal output)
    --exclude dir  Exclude specified directory from search

If directory is not specified, current directory is used.
EOF
    exit 1
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
while [ $# -gt 0 ]; do
    case "$1" in
        --help|-h)
            usage
            ;;
        --dry-run|-n)
            dry_run=1
            ;;
        --force|-f)
            force=1
            ;;
        --quiet|-q)
            quiet=1
            ;;
        --exclude)
            shift
            if [ $# -eq 0 ]; then
                error "--exclude requires a directory argument"
                exit 1
            fi
            exclude_dirs="$exclude_dirs${exclude_dirs:+ -o -name }'$1'"
            ;;
        -*)
            error "Unknown option: $1"
            usage
            ;;
        *)
            if [ -n "$target_dir" ] && [ "$target_dir" != "." ]; then
                error "Multiple directory arguments specified"
                usage
            fi
            target_dir="$1"
            ;;
    esac
    shift
done

# Check if directory parameter is provided
if [ ! -d "$target_dir" ]; then
    error "Directory '$target_dir' does not exist"
    exit 1
fi

# Change to target directory
cd "$target_dir" || {
    error "Cannot access directory '$target_dir'"
    exit 1
}

# Create temporary files for storing results
tmp_count=$(mktemp)
tmp_size=$(mktemp)
trap 'rm -f $tmp_count $tmp_size' EXIT

# Initializs counters in files
echo "0" > "$tmp_count"
echo "0" > "$tmp_size"

# First pass: count and display files
if [ $quiet -eq 0 ]; then
    printf "Scanning for macOS-specific files in '%s'...\n" "$target_dir"
    if [ -n "$exclude_dirs" ]; then
        printf "Excluding directories matching: %s\n" "$exclude_dirs"
    fi
fi

# Construct find command with directory exclusion
if [ -n "$exclude_dirs" ]; then
    find_cmd="find . -type d \( -name ${exclude_dirs} \) -prune -o \( \
        -name '.DS_Store' -o \
        -name '._*' -o \
        -name '.AppleDouble' -o \
        -name '.AppleDB' -o \
        -name '.AppleDesktop' -o \
        -name '.Trashes' \
        \) -type f -print"
else
    find_cmd="find . \( \
        -name '.DS_Store' -o \
        -name '._*' -o \
        -name '.AppleDouble' -o \
        -name '.AppleDB' -o \
        -name '.AppleDesktop' -o \
        -name '.Trashes' \
        \) -type f -print"
fi

# Execute the find command
eval "$find_cmd" | while IFS= read -r file; do
    size=$(stat -f %z "$file" 2>/dev/null || stat -c %s "$file" 2>/dev/null || echo "0")
    case "$size" in
        ''|*[!0-9]*) size=0 ;;
    esac
    count=$(cat "$tmp_count")
    total_size=$(cat "$tmp_size")
    echo "$((count + 1))" > "$tmp_count"
    echo "$((total_size + size))" > "$tmp_size"
    if [ $quiet -eq 0 ]; then
        printf "File: %s (Size: %s)\n" "$file" "$(format_size "$size")"
    fi
done

# Get final counts
total_files=$(cat "$tmp_count")
total_size=$(cat "$tmp_size")

# Check if any files were found
if [ "$total_files" -eq 0 ]; then
    if [ $quiet -eq 0 ]; then
        printf "No macOS-specific files found.\n"
    fi
    exit 0
fi

# Show summary
if [ $quiet -eq 0 ]; then
    printf "\nSummary:\n"
    printf "Files: %d\n" "$total_files"
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

# Delete files if not in dry run mode
if [ $dry_run -eq 1 ]; then
    if [ $quiet -eq 0 ]; then
        printf "\nDry run - no files will be deleted.\n"
    fi
else
    if [ $quiet -eq 0 ]; then
        printf "\nDeleting files...\n"
    fi

    # Use the same find command for deletion
    if [ -n "$exclude_dirs" ]; then
        eval "$find_cmd -exec rm -f {} + 2>/dev/null" || {
            error "Some files could not be deleted (permission denied)"
            exit 1
        }
    else
        eval "$find_cmd -exec rm -f {} + 2>/dev/null" || {
            error "Some files could not be deleted (permission denied)"
            exit 1
        }
    fi

    if [ $quiet -eq 0 ]; then
        printf "Cleanup completed successfully.\n"
    fi
fi