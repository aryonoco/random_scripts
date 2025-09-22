function flac2aac
    # Parse arguments with defaults
    argparse 'q/quality=' 'd/delete' 'p/parallel' 'e/encoder=' 'h/help' -- $argv
    or return

    # Show help
    if set -q _flag_help
        echo "Usage: flac2aac [-q quality] [-d] [-p] [-e encoder] [-h]"
        echo "  -q/--quality:  VBR quality (default: 2 for Apple, 4 for FDK, 0 for native)"
        echo "  -d/--delete:   Delete original FLAC files after conversion"
        echo "  -p/--parallel: Use parallel processing (all CPU cores)"
        echo "  -e/--encoder:  Force specific encoder (aac_at/libfdk_aac/aac)"
        echo "  -h/--help:     Show this help"
        return 0
    end

    # Detect OS and choose best encoder
    set os (uname)
    set encoder ""
    set quality_default 2
    set quality_param "-q:a"

    if set -q _flag_encoder
        # User specified encoder
        set encoder $_flag_encoder
    else
        # Auto-detect best encoder
        switch $os
            case Darwin
                # macOS - use Apple AudioToolbox
                if ffmpeg -encoders 2>/dev/null | grep -q aac_at
                    set encoder "aac_at"
                    set quality_default 2
                    echo (set_color green)"✓ Using Apple AudioToolbox encoder (best quality)"(set_color normal)
                else
                    echo (set_color yellow)"⚠ Apple encoder not found, falling back..."(set_color normal)
                end

            case Linux FreeBSD
                # Linux/BSD - prefer FDK-AAC, fallback to native
                if ffmpeg -encoders 2>/dev/null | grep -q libfdk_aac
                    set encoder "libfdk_aac"
                    set quality_default 4
                    set quality_param "-vbr"
                    echo (set_color green)"✓ Using FDK-AAC encoder (best open-source)"(set_color normal)
                else if ffmpeg -encoders 2>/dev/null | grep -q aac
                    set encoder "aac"
                    set quality_default 0
                    set quality_param "-q:a"
                    echo (set_color yellow)"⚠ Using native FFmpeg AAC encoder"(set_color normal)
                end
        end
    end

    # Fallback if no encoder found
    if test -z "$encoder"
        set encoder "aac"
        set quality_default 0
        echo (set_color red)"⚠ Using fallback AAC encoder"(set_color normal)
    end

    # Set quality
    set quality (test -n "$_flag_quality" && echo $_flag_quality || echo $quality_default)

    # Build ffmpeg codec parameters based on encoder
    set codec_params "-c:a $encoder"
    switch $encoder
        case aac_at
            # Apple AudioToolbox: -q:a 0-14 (2 recommended)
            set codec_params "$codec_params -q:a $quality"
            echo "Quality range: 0-14 (using: $quality)"

        case libfdk_aac
            # FDK-AAC: -vbr 1-5 (4 recommended)
            # VBR modes: 1=~32kbps, 2=~48kbps, 3=~64kbps, 4=~128kbps, 5=~192kbps
            set codec_params "$codec_params -vbr $quality"
            echo "Quality range: 1-5 (using: $quality)"

        case aac
            # Native FFmpeg: -q:a 0.1-2 (0.3 recommended) or -b:a for CBR
            if test -n "$_flag_quality"
                set codec_params "$codec_params -q:a $quality"
                echo "Quality range: 0.1-2 (using: $quality)"
            else
                # Use CBR for native encoder as more reliable
                set codec_params "$codec_params -b:a 192k"
                echo "Using CBR 192k (specify -q for VBR)"
            end
    end

    # Find all FLAC files
    set flac_files **.flac

    if test (count $flac_files) -eq 0
        echo "No FLAC files found"
        return 1
    end

    echo "Found "(set_color yellow)(count $flac_files)(set_color normal)" FLAC files"
    echo ""

    # Function for single file conversion (used by both serial and parallel)
    function _convert_single_file
        set f $argv[1]
        set codec_params $argv[2]
        set delete_flag $argv[3]

        set output (string replace ".flac" ".m4a" "$f")

        if test -f "$output"
            echo (set_color yellow)"⚠ Skipping: $f (exists)"(set_color normal)
            return 2
        end

        set output_dir (dirname "$output")
        test -d "$output_dir" || mkdir -p "$output_dir"

        echo (set_color blue)"Converting: $f"(set_color normal)

        if ffmpeg -i "$f" $codec_params -map_metadata 0 -loglevel warning "$output"
            echo (set_color green)"✓ Completed: $output"(set_color normal)
            test "$delete_flag" = "yes" && rm "$f"
            return 0
        else
            echo (set_color red)"✗ Failed: $f"(set_color normal)
            return 1
        end
    end

    # Export function for parallel processing
    if set -q _flag_parallel
        echo (set_color cyan)"Using parallel processing..."(set_color normal)

        # Determine number of CPU cores
        switch $os
            case Darwin
                set cores (sysctl -n hw.ncpu)
            case Linux
                set cores (nproc)
            case '*'
                set cores 4
        end

        echo "Processing with $cores cores"
        echo ""

        # Create a temporary script for parallel execution
        set delete_flag (set -q _flag_delete && echo "yes" || echo "no")

        # Use GNU parallel if available, otherwise fallback to xargs
        if command -v parallel >/dev/null
            # GNU Parallel version (better progress tracking)
            printf '%s\n' $flac_files | parallel -j $cores --bar "
                set output (string replace '.flac' '.m4a' '{}')
                test -f \"\$output\" && exit 0
                set output_dir (dirname \"\$output\")
                test -d \"\$output_dir\" || mkdir -p \"\$output_dir\"
                ffmpeg -i '{}' $codec_params -map_metadata 0 -loglevel warning \"\$output\" && echo '✓ {}' || echo '✗ {}'
                test '$delete_flag' = 'yes' && test -f \"\$output\" && rm '{}'
            "
        else
            # xargs version
            printf '%s\0' $flac_files | xargs -0 -n 1 -P $cores -I {} fish -c "
                set output (string replace '.flac' '.m4a' '{}')
                test -f \"\$output\" && exit 0
                set output_dir (dirname \"\$output\")
                test -d \"\$output_dir\" || mkdir -p \"\$output_dir\"
                ffmpeg -i '{}' $codec_params -map_metadata 0 -loglevel warning \"\$output\"
                test '$delete_flag' = 'yes' && test -f \"\$output\" && rm '{}'
            "
        end

    else
        # Serial processing with statistics
        set successful 0
        set failed 0
        set skipped 0
        set delete_flag (set -q _flag_delete && echo "yes" || echo "no")

        for f in $flac_files
            _convert_single_file "$f" "$codec_params" "$delete_flag"

            switch $status
                case 0
                    set successful (math $successful + 1)
                case 1
                    set failed (math $failed + 1)
                case 2
                    set skipped (math $skipped + 1)
            end
        end

        # Summary
        echo ""
        echo (set_color green)"Summary:"(set_color normal)
        echo "  Successful: "(set_color green)$successful(set_color normal)
        echo "  Failed:     "(set_color red)$failed(set_color normal)
        echo "  Skipped:    "(set_color yellow)$skipped(set_color normal)
    end
end
