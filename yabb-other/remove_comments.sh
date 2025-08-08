#!/usr/bin/env bash

# Script to remove comments from yabb.sh
# Preserves shebang and handles various comment types

input_file="yabb.sh"
output_file="yabb_no_comments.sh"

if [[ ! -f "$input_file" ]]; then
    echo "Error: $input_file not found"
    exit 1
fi

# Process the file
{
    # Keep the shebang
    head -n1 "$input_file"
    
    # Process rest of file, removing comments
    tail -n +2 "$input_file" | sed -E '
        # Remove whole-line comments (lines that start with #)
        /^[[:space:]]*#/d
        
        # Remove inline comments (preserving strings)
        s/([^"'"'"']*)#[^"'"'"']*$/\1/
        
        # Remove section separator comments
        /^[[:space:]]*#{10,}/d
        
        # Clean up trailing whitespace
        s/[[:space:]]+$//
    ' | awk '
        # Remove empty lines that were created by comment removal
        # but keep single empty lines for readability
        BEGIN { prev_empty = 0 }
        {
            if (NF == 0) {
                if (!prev_empty) {
                    print ""
                    prev_empty = 1
                }
            } else {
                print $0
                prev_empty = 0
            }
        }
    '
} > "$output_file"

echo "Created $output_file without comments"
chmod +x "$output_file"