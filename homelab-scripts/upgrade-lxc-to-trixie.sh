#!/bin/bash
#
# LXC Container Upgrade Script: Debian 12 (Bookworm) → 13 (Trixie)
# Run this script INSIDE the LXC container as root
#
# Usage: bash upgrade-lxc-to-trixie.sh
#

set -e  # Exit on any error

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Log file
LOGFILE="/var/log/trixie-upgrade-$(date +%Y%m%d-%H%M%S).log"

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

# Function to check if running as root
check_root() {
    if [ "$EUID" -ne 0 ]; then
        error "This script must be run as root"
        exit 1
    fi
}

# Function to check if running Bookworm
check_bookworm() {
    if ! grep -q "bookworm" /etc/apt/sources.list 2>/dev/null; then
        error "This doesn't appear to be a Bookworm system"
        error "Current /etc/apt/sources.list:"
        cat /etc/apt/sources.list
        exit 1
    fi

    DEBIAN_VERSION=$(cat /etc/debian_version)
    log "Current Debian version: $DEBIAN_VERSION"

    if [[ ! "$DEBIAN_VERSION" =~ ^12 ]]; then
        warning "Debian version doesn't start with 12. Proceeding anyway..."
    fi
}

# Function to check disk space
check_disk_space() {
    AVAILABLE=$(df / | awk 'NR==2 {print $4}')
    AVAILABLE_GB=$((AVAILABLE / 1024 / 1024))

    log "Available disk space: ${AVAILABLE_GB}GB"

    if [ "$AVAILABLE_GB" -lt 2 ]; then
        error "Insufficient disk space. Need at least 2GB free, have ${AVAILABLE_GB}GB"
        exit 1
    fi
}

# Set non-interactive mode for apt
export DEBIAN_FRONTEND=noninteractive
export APT_LISTCHANGES_FRONTEND=none
export NEEDRESTART_MODE=a

# APT options for non-interactive upgrade
# --force-confnew = Always install new config files from packages
APT_OPTS="-y -o Dpkg::Options::=--force-confdef -o Dpkg::Options::=--force-confnew"

log "========================================="
log "LXC Bookworm to Trixie Upgrade Starting"
log "========================================="
log "Configuration: Installing NEW config files (replacing old ones)"
log ""

# Pre-flight checks
log "Running pre-flight checks..."
check_root
check_bookworm
check_disk_space

# Step 1: Update and upgrade current Bookworm system
log "Step 1/8: Updating current Bookworm system..."
apt update 2>&1 | tee -a "$LOGFILE"
apt upgrade $APT_OPTS 2>&1 | tee -a "$LOGFILE"
apt full-upgrade $APT_OPTS 2>&1 | tee -a "$LOGFILE"
apt autoremove $APT_OPTS 2>&1 | tee -a "$LOGFILE"
apt autoclean 2>&1 | tee -a "$LOGFILE"

log "Step 2/8: Ensuring package system is consistent..."
dpkg --configure -a 2>&1 | tee -a "$LOGFILE"
apt -f install $APT_OPTS 2>&1 | tee -a "$LOGFILE"

# Step 3: Backup and update sources.list
log "Step 3/8: Backing up and updating sources.list..."
cp /etc/apt/sources.list /etc/apt/sources.list.bookworm-backup
log "Backup created: /etc/apt/sources.list.bookworm-backup"

sed -i 's/bookworm/trixie/g' /etc/apt/sources.list
log "Updated sources.list to Trixie repositories"

# Show what changed
log "New sources.list contents:"
cat /etc/apt/sources.list | tee -a "$LOGFILE"

# Step 4: Update package index with Trixie repos
log "Step 4/8: Updating package index with Trixie repositories..."
apt update 2>&1 | tee -a "$LOGFILE"

# Step 5: Minimal upgrade (without new packages)
log "Step 5/8: Performing minimal upgrade..."
apt upgrade --without-new-pkgs $APT_OPTS 2>&1 | tee -a "$LOGFILE"

# Step 6: Full upgrade to Trixie
log "Step 6/8: Performing full upgrade to Trixie..."
log "This may take 10-30 minutes depending on container size..."
apt full-upgrade $APT_OPTS 2>&1 | tee -a "$LOGFILE"

# Step 7: Clean up
log "Step 7/8: Cleaning up..."
apt autoremove $APT_OPTS 2>&1 | tee -a "$LOGFILE"
apt autoclean 2>&1 | tee -a "$LOGFILE"
dpkg --configure -a 2>&1 | tee -a "$LOGFILE"
apt -f install $APT_OPTS 2>&1 | tee -a "$LOGFILE"

# Step 8: Modernize sources
log "Step 8/8: Modernizing APT source format..."
# Auto-answer 'Y' to apply changes
echo "Y" | apt modernize-sources 2>&1 | tee -a "$LOGFILE"

# Final verification
log "========================================="
log "Upgrade Complete! Verifying..."
log "========================================="

NEW_VERSION=$(cat /etc/debian_version)
log "New Debian version: $NEW_VERSION"

if [[ "$NEW_VERSION" =~ ^13 ]] || [[ "$NEW_VERSION" =~ trixie ]]; then
    log "${GREEN}✓ Successfully upgraded to Debian 13 (Trixie)${NC}"
else
    warning "Version check inconclusive. Manual verification recommended."
fi

# Check for failed services
log "Checking for failed services..."
FAILED=$(systemctl --failed --no-pager --no-legend | wc -l)
if [ "$FAILED" -gt 0 ]; then
    warning "Found $FAILED failed service(s):"
    systemctl --failed --no-pager | tee -a "$LOGFILE"
else
    log "${GREEN}✓ All services running normally${NC}"
fi

# Check for packages in bad state
log "Checking for packages in inconsistent state..."
BAD_PKGS=$(dpkg -l | grep -E "^iU|^iF" | wc -l)
if [ "$BAD_PKGS" -gt 0 ]; then
    warning "Found $BAD_PKGS package(s) in inconsistent state:"
    dpkg -l | grep -E "^iU|^iF" | tee -a "$LOGFILE"
else
    log "${GREEN}✓ All packages in good state${NC}"
fi

log "========================================="
log "Upgrade Summary"
log "========================================="
log "Previous version: Debian $DEBIAN_VERSION (Bookworm)"
log "Current version:  Debian $NEW_VERSION (Trixie)"
log "Log file: $LOGFILE"
log "Backup sources: /etc/apt/sources.list.bookworm-backup"
log ""
log "${YELLOW}RECOMMENDED NEXT STEPS:${NC}"
log "1. Review the log file: cat $LOGFILE"
log "2. Check service status: systemctl status"
log "3. Test your applications"
log "4. Reboot container when ready: systemctl reboot"
log "   (Or from Proxmox host: pct reboot <CTID>)"
log "========================================="

exit 0
