function upgrade-npm-globals --description "Upgrade all globally installed packages across npm, pnpm, bun, and deno"
    # Initialize counters
    set -l total_upgraded 0
    set -l total_failed 0
    set -l total_skipped 0

    # Colors
    set -l reset (set_color normal)
    set -l green (set_color green)
    set -l blue (set_color blue)
    set -l yellow (set_color yellow)
    set -l red (set_color red)
    set -l dim (set_color 666)

    # Spinner characters
    set -l spinner_chars ⠋ ⠙ ⠹ ⠸ ⠼ ⠴ ⠦ ⠧ ⠇ ⠏

    # Helper functions
    function __show_spinner --argument-names message
        # Pass spinner_chars as inherit-variable to make it accessible
        set -l spinner_chars $spinner_chars
        if test (count $spinner_chars) -eq 0
            # Fallback if spinner_chars is empty
            printf '\r%s %s ' $blue"..."$dim $message$reset
        else
            set -l frame_index (math (date +%s) % (count $spinner_chars) + 1)
            printf '\r%s %s ' $blue$spinner_chars[$frame_index]$dim $message$reset
        end
    end

    function __clear_line
        printf '\r%s\r' (string repeat -n 80 ' ')
    end

    function __log_result --argument-names status_code package_name
        __clear_line
        if test $status_code -eq 0
            set -g total_upgraded (math $total_upgraded + 1)
            echo $green"  ✓ Updated $package_name"$reset
        else
            set -g total_failed (math $total_failed + 1)
            echo $red"  ✗ Failed to update $package_name"$reset
        end
    end

    echo $blue"🚀 Checking for global package updates..."$reset
    echo

    # NPM Packages
    if command -sq npm
        echo $green"📦 NPM"$reset
        set -lx NPM_CONFIG_FUND false

        # Update npm itself first
        __show_spinner "Updating npm..."
        npm install -g npm@latest --loglevel error >/dev/null 2>&1
        __log_result $status "npm (package manager)"

        # Use npm outdated to find packages needing updates
        set -l npm_outdated_raw (npm outdated -g --parseable --depth=0 2>/dev/null)
        set -l outdated_packages

        # Parse the npm outdated output
        for line in $npm_outdated_raw
            # Extract package name from format: /path:package@version:package@current:package@wanted:location
            set -l package_info (string split ":" $line)
            if test (count $package_info) -ge 2
                # Get the package name without version from the second field
                set -l package_with_version $package_info[2]
                set -l package_name (string replace -r '@[0-9]+\.[0-9]+\.[0-9]+.*$' '' "$package_with_version")
                if test -n "$package_name"
                    set -a outdated_packages $package_name
                end
            end
        end

        if test (count $outdated_packages) -gt 0
            echo $dim"  Found "(count $outdated_packages)" outdated packages"$reset
            for package_name in $outdated_packages
                __show_spinner "Updating $package_name..."
                npm install -g "$package_name@latest" --loglevel error >/dev/null 2>&1
                __log_result $status $package_name
            end
        else
            echo $dim"  ✓ All packages are already up-to-date"$reset
        end
    else
        echo $yellow"→ Skipping npm (not found)"$reset
        set total_skipped (math $total_skipped + 1)
    end
    echo

    # PNPM
    if command -sq pnpm
        echo $green"📦 PNPM"$reset

        # Check if pnpm has any global packages
        set -l pnpm_packages (pnpm list -g --depth=0 --json 2>/dev/null)

        if test -z "$pnpm_packages"; or string match -q "*\[\]" "$pnpm_packages"
            echo $dim"  No global packages installed"$reset
        else
            __show_spinner "Updating all global packages..."
            set -l pnpm_output (pnpm up -g --latest 2>&1)
            set -l pnpm_status $status
            __clear_line

            if test $pnpm_status -eq 0
                if string match -q "*All dependencies are up to date*" "$pnpm_output"
                    echo $dim"  ✓ All packages are already up-to-date"$reset
                else
                    echo $green"  ✓ All packages updated to latest"$reset
                    set total_upgraded (math $total_upgraded + 1)
                end
            else
                # Check for common error cases
                if string match -q "*No package.json found*" "$pnpm_output"
                    echo $dim"  No global packages to update"$reset
                else
                    echo $red"  ✗ Update failed"$reset
                    set total_failed (math $total_failed + 1)
                end
            end
        end
    else
        echo $yellow"→ Skipping pnpm (not found)"$reset
        set total_skipped (math $total_skipped + 1)
    end
    echo

    # Bun
    if command -sq bun
        echo $green"📦 Bun"$reset

        # Try bun upgrade --global first (future compatibility)
        __show_spinner "Checking for bun upgrade command..."
        if bun upgrade --help 2>&1 | string match -q '*global*'
            __show_spinner "Updating all global packages..."
            set -l bun_output (bun upgrade --global 2>&1)
            set -l bun_status $status
            __clear_line

            if test $bun_status -eq 0
                if string match -q "*No packages to update*" "$bun_output"
                    echo $dim"  ✓ All packages are already up-to-date"$reset
                else
                    echo $green"  ✓ All packages updated"$reset
                    set total_upgraded (math $total_upgraded + 1)
                end
            else
                set -l fallback_needed true
            end
        else
            set -l fallback_needed true
        end

        # Fallback to manual update
        if set -q fallback_needed
            __clear_line
            echo $dim"  Note: Using manual update (bun upgrade --global not available)"$reset

            set -l bun_packages (
                bun pm ls -g 2>/dev/null |
                string match -r '^\│\s+([^@\s│]+)' |
                string replace -r '^\│\s+' '' |
                string trim |
                string match -v -r '^(Name|─)'
            )

            if test (count $bun_packages) -gt 0
                echo $dim"  Found "(count $bun_packages)" packages"$reset
                for package in $bun_packages
                    if test -n "$package"
                        __show_spinner "Updating $package..."
                        bun add -g "$package@latest" >/dev/null 2>&1
                        __log_result $status $package
                    end
                end
            else
                echo $dim"  No packages found"$reset
            end
        end
    else
        echo $yellow"→ Skipping bun (not found)"$reset
        set total_skipped (math $total_skipped + 1)
    end
    echo

    # Deno
    if command -sq deno
        echo $green"📦 Deno"$reset
        __show_spinner "Upgrading Deno runtime..."
        set -l deno_output (deno upgrade 2>&1)
        set -l deno_status $status
        __clear_line

        if test $deno_status -eq 0
            if string match -q "*already the latest*" "$deno_output"
                echo $dim"  ✓ Already at latest version"$reset
            else
                echo $green"  ✓ Deno runtime upgraded"$reset
                set total_upgraded (math $total_upgraded + 1)
            end
        else
            # Check if deno was installed without upgrade feature
            if string match -q "*built without the \"upgrade\" feature*" "$deno_output"
                echo $dim"  Note: Deno installed via package manager (use package manager to upgrade)"$reset
            else
                echo $red"  ✗ Upgrade failed"$reset
                set total_failed (math $total_failed + 1)
            end
        end
    else
        echo $yellow"→ Skipping deno (not found)"$reset
        set total_skipped (math $total_skipped + 1)
    end
    echo

    # Summary
    echo $blue"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"$reset

    if test $total_failed -gt 0
        echo $yellow"⚠️  Upgrade complete with issues"$reset
    else if test $total_upgraded -gt 0
        echo $green"✅ All upgrades completed successfully!"$reset
    else
        echo $green"✅ Everything is already up-to-date!"$reset
    end

    # Statistics
    set -l stats
    if test $total_upgraded -gt 0; set -a stats $green"Upgraded: $total_upgraded"$reset; end
    if test $total_failed -gt 0; set -a stats $red"Failed: $total_failed"$reset; end
    if test $total_skipped -gt 0; set -a stats $dim"Skipped: $total_skipped"$reset; end

    if test (count $stats) -gt 0
        echo "   "(string join " | " $stats)
    end
end

# Short abbreviation
abbr -a -g ugp upgrade-all-global-packages
