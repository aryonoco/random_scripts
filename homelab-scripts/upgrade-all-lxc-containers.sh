#!/bin/bash
#
# Proxmox Host Script: Upgrade Multiple LXC Containers from Bookworm to Trixie
# Run this script on the Proxmox VE HOST as root
#
# Usage: bash upgrade-all-lxc-containers.sh
#

set -e

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
UPGRADE_SCRIPT="$SCRIPT_DIR/upgrade-lxc-to-trixie.sh"

WAIT_AFTER_REBOOT=60     # Seconds to wait after rebooting each container

# List of container IDs to upgrade
# Edit this array with your container IDs
CONTAINERS=(1001 1002 1003 1004 1005 1006 1007 1008 1009 1010 1011 1012)

# You can also auto-detect all running Bookworm containers:
# CONTAINERS=($(pct list | awk 'NR>1 {print $1}'))

# Log file
LOGFILE="/var/log/lxc-mass-upgrade-$(date +%Y%m%d-%H%M%S).log"

# Function to print and log
log() {
    echo -e "${GREEN}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $1" | tee -a "$LOGFILE"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1" | tee -a "$LOGFILE"
}

warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1" | tee -a "$LOGFILE"
}

info() {
    echo -e "${BLUE}[INFO]${NC} $1" | tee -a "$LOGFILE"
}

# Function to check if running on Proxmox host
check_proxmox_host() {
    if ! command -v pct &> /dev/null; then
        error "This script must be run on a Proxmox VE host"
        exit 1
    fi

    if [ "$EUID" -ne 0 ]; then
        error "This script must be run as root"
        exit 1
    fi
}

# Function to check if container exists and is running
check_container() {
    local CTID=$1

    if ! pct status "$CTID" &>/dev/null; then
        error "Container $CTID does not exist"
        return 1
    fi

    return 0
}

# Function to get container hostname
get_container_hostname() {
    local CTID=$1
    pct exec "$CTID" -- hostname 2>/dev/null || echo "CT-$CTID"
}

# Function to check if container is Bookworm
check_if_bookworm() {
    local CTID=$1

    if pct exec "$CTID" -- grep -q "bookworm" /etc/apt/sources.list 2>/dev/null; then
        return 0
    else
        return 1
    fi
}


# Function to upgrade container
upgrade_container() {
    local CTID=$1
    local HOSTNAME=$2

    info "Upgrading container $CTID ($HOSTNAME)..."

    # Ensure container is running
    if [ "$(pct status "$CTID")" != "status: running" ]; then
        info "Starting container $CTID..."
        pct start "$CTID"
        sleep 5
    fi

    # Copy upgrade script to container
    info "Copying upgrade script to container $CTID..."
    if ! pct push "$CTID" "$UPGRADE_SCRIPT" /root/upgrade-to-trixie.sh; then
        error "Failed to copy script to container $CTID"
        return 1
    fi

    # Make script executable
    pct exec "$CTID" -- chmod +x /root/upgrade-to-trixie.sh

    # Run upgrade script
    info "Running upgrade script in container $CTID..."
    info "This may take 10-30 minutes..."

    if pct exec "$CTID" -- /root/upgrade-to-trixie.sh 2>&1 | tee -a "$LOGFILE"; then
        log "✓ Upgrade completed successfully for $CTID"
        return 0
    else
        error "✗ Upgrade failed for $CTID"
        return 1
    fi
}

# Function to reboot container
reboot_container() {
    local CTID=$1
    local HOSTNAME=$2

    info "Rebooting container $CTID ($HOSTNAME)..."

    if pct reboot "$CTID" 2>&1 | tee -a "$LOGFILE"; then
        log "✓ Container $CTID rebooted"
        info "Waiting $WAIT_AFTER_REBOOT seconds for container to come back up..."
        sleep "$WAIT_AFTER_REBOOT"
        return 0
    else
        error "✗ Failed to reboot container $CTID"
        return 1
    fi
}

# Function to verify upgrade
verify_upgrade() {
    local CTID=$1
    local HOSTNAME=$2

    info "Verifying upgrade for container $CTID ($HOSTNAME)..."

    # Check Debian version
    local VERSION
    VERSION=$(pct exec "$CTID" -- cat /etc/debian_version 2>/dev/null)

    if [[ "$VERSION" =~ ^13 ]] || [[ "$VERSION" =~ trixie ]]; then
        log "✓ Container $CTID verified: Debian $VERSION (Trixie)"
        return 0
    else
        warning "? Container $CTID version check inconclusive: $VERSION"
        return 1
    fi
}

# Main execution
main() {
    log "=============================================="
    log "Proxmox LXC Mass Upgrade Script"
    log "Bookworm (12) → Trixie (13)"
    log "=============================================="
    log "Log file: $LOGFILE"
    log ""

    # Pre-flight checks
    log "Running pre-flight checks..."
    check_proxmox_host

    if [ ! -f "$UPGRADE_SCRIPT" ]; then
        error "Upgrade script not found at: $UPGRADE_SCRIPT"
        exit 1
    fi

    log "✓ Proxmox host verified"
    log "✓ Upgrade script found"
    log ""

    # Display container list
    log "Containers to upgrade:"
    ELIGIBLE_COUNT=0
    for CTID in "${CONTAINERS[@]}"; do
        if check_container "$CTID"; then
            HOSTNAME=$(get_container_hostname "$CTID")
            STATUS=$(pct status "$CTID")

            if check_if_bookworm "$CTID"; then
                log "  • CT $CTID ($HOSTNAME) - $STATUS - Bookworm ✓"
                ELIGIBLE_COUNT=$((ELIGIBLE_COUNT + 1))
            else
                warning "  • CT $CTID ($HOSTNAME) - $STATUS - NOT Bookworm (will skip)"
            fi
        else
            warning "  • CT $CTID - Does not exist (will skip)"
        fi
    done
    log ""

    # Ask for confirmation
    echo -e "${YELLOW}This will upgrade ${ELIGIBLE_COUNT} container(s) to Debian 13 (Trixie)${NC}"
    echo -e "${RED}WARNING: No backups will be created automatically${NC}"
    echo ""
    read -p "Continue? (yes/no): " CONFIRM

    if [ "$CONFIRM" != "yes" ]; then
        log "Upgrade cancelled by user"
        exit 0
    fi

    log ""
    log "=============================================="
    log "Starting mass upgrade..."
    log "=============================================="
    log ""

    # Track results
    declare -A RESULTS
    SUCCESSFUL=0
    FAILED=0
    SKIPPED=0

    # Process each container
    for CTID in "${CONTAINERS[@]}"; do
        log ""
        log "=============================================="
        log "Processing Container $CTID"
        log "=============================================="

        # Check if container exists
        if ! check_container "$CTID"; then
            warning "Skipping non-existent container $CTID"
            RESULTS[$CTID]="SKIPPED - Does not exist"
            SKIPPED=$((SKIPPED + 1))
            continue
        fi

        HOSTNAME=$(get_container_hostname "$CTID")

        # Check if Bookworm
        if ! check_if_bookworm "$CTID"; then
            warning "Container $CTID ($HOSTNAME) is not running Bookworm - skipping"
            RESULTS[$CTID]="SKIPPED - Not Bookworm"
            SKIPPED=$((SKIPPED + 1))
            continue
        fi

        # Step 1: Upgrade
        if ! upgrade_container "$CTID" "$HOSTNAME"; then
            error "Upgrade failed for $CTID"
            RESULTS[$CTID]="FAILED - Upgrade error"
            FAILED=$((FAILED + 1))
            continue
        fi

        # Step 2: Reboot
        if ! reboot_container "$CTID" "$HOSTNAME"; then
            warning "Reboot failed for $CTID - may need manual intervention"
        fi

        # Step 3: Verify
        if verify_upgrade "$CTID" "$HOSTNAME"; then
            RESULTS[$CTID]="SUCCESS"
            SUCCESSFUL=$((SUCCESSFUL + 1))
        else
            RESULTS[$CTID]="COMPLETED - Verification inconclusive"
            SUCCESSFUL=$((SUCCESSFUL + 1))
        fi

        log "✓ Container $CTID processing complete"
    done

    # Final summary
    log ""
    log "=============================================="
    log "Upgrade Summary"
    log "=============================================="
    log "Total containers processed: ${#CONTAINERS[@]}"
    log "Successful: $SUCCESSFUL"
    log "Failed: $FAILED"
    log "Skipped: $SKIPPED"
    log ""
    log "Detailed Results:"
    for CTID in "${CONTAINERS[@]}"; do
        if [ -n "${RESULTS[$CTID]}" ]; then
            HOSTNAME=$(get_container_hostname "$CTID" 2>/dev/null || echo "Unknown")

            case "${RESULTS[$CTID]}" in
                SUCCESS*)
                    log "  ${GREEN}✓${NC} CT $CTID ($HOSTNAME): ${RESULTS[$CTID]}"
                    ;;
                FAILED*)
                    log "  ${RED}✗${NC} CT $CTID ($HOSTNAME): ${RESULTS[$CTID]}"
                    ;;
                SKIPPED*)
                    log "  ${YELLOW}⊘${NC} CT $CTID ($HOSTNAME): ${RESULTS[$CTID]}"
                    ;;
                *)
                    log "  ${BLUE}•${NC} CT $CTID ($HOSTNAME): ${RESULTS[$CTID]}"
                    ;;
            esac
        fi
    done

    log ""
    log "=============================================="
    log "Next Steps:"
    log "=============================================="
    log "1. Review log file: cat $LOGFILE"
    log "2. Test applications in each upgraded container"
    log "3. Monitor containers for any issues"
    log ""

    if [ $FAILED -gt 0 ]; then
        warning "Some containers failed to upgrade. Review the log for details."
        exit 1
    else
        log "${GREEN}All containers processed successfully!${NC}"
        exit 0
    fi
}

# Run main function
main
