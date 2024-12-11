#!/bin/sh

# Function to display usage instructions
show_usage() {
    echo "Usage: $0 extension [directory] [-R]"
    echo "Examples:"
    echo "  $0 aax                    # Delete .aax files in current directory"
    echo "  $0 aax -R                 # Delete .aax files in current directory and subdirectories"
    echo "  $0 aax /path/to/dir       # Delete .aax files in specified directory"
    echo "  $0 aax /path/to/dir -R    # Delete .aax files in specified directory and subdirectories"
    exit 1
}

# Initialize variables
recursive=false
directory="."
extension=""

# Parse command line arguments
while [ $# -gt 0 ]; do
    case $1 in
        -R)
            recursive=true
            shift
            ;;
        -*)
            echo "Error: Unknown option $1"
            show_usage
            ;;
        *)
            if [ -z "$extension" ]; then
                extension="$1"
            elif [ "$directory" = "." ]; then
                directory="$1"
            else
                echo "Error: Too many arguments"
                show_usage
            fi
            shift
            ;;
    esac
done

# Check if extension was provided
if [ -z "$extension" ]; then
    echo "Error: Extension must be specified"
    show_usage
fi

# Verify directory exists and is accessible
if [ ! -d "$directory" ]; then
    echo "Error: Directory '$directory' does not exist or is not accessible"
    exit 1
fi

# Set find command depth based on recursive flag
if [ "$recursive" = true ]; then
    depth_opt=""
    echo "Searching recursively in $directory"
else
    depth_opt="-maxdepth 1"
    echo "Searching only in $directory (not recursive)"
fi

# Count matching files
count=$(find "$directory" $depth_opt -type f -name "*.$extension" | wc -l)
count=$(echo "$count" | tr -d ' ') # Remove whitespace

# Check if any matching files were found
if [ "$count" -eq 0 ]; then
    echo "No files found with extension .$extension"
    exit 0
fi

# Display files to be deleted and request confirmation
echo "The following files will be deleted:"
find "$directory" $depth_opt -type f -name "*.$extension" -print
echo "Total files to be deleted: $count"

read -p "Proceed with deletion? (y/n): " confirm

if [ "$confirm" = "y" ] || [ "$confirm" = "Y" ]; then
    # Calculate total space before deletion
    total_space=$(find "$directory" $depth_opt -type f -name "*.$extension" -exec du -ch {} + | grep total | cut -f1)

    # Delete files
    find "$directory" $depth_opt -type f -name "*.$extension" -exec rm -f {} \;

    # Display operation statistics
    echo "Operation completed:"
    echo "- Base directory: $directory"
    echo "- Recursive mode: $recursive"
    echo "- Files deleted: $count"
    echo "- Total space freed: $total_space"
else
    # Operation cancelled by user
    echo "Operation cancelled"
    echo "- Base directory: $directory"
    echo "- Recursive mode: $recursive"
    echo "- Files retained: $count"
    exit 0
fi