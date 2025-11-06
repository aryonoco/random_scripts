#!/bin/bash
# Simple Docker CE to Debian migration script
# No fancy logging, just gets the job done

set -e

DRY_RUN="${DRY_RUN:-false}"

echo "=== Docker CE to Debian Migration ==="
echo "DRY_RUN: $DRY_RUN"
echo ""

# Get list of running containers with Docker CE
for vmid in $(pct list | awk 'NR>1 && $2=="running" {print $1}'); do
    # Check if docker-ce is installed
    if pct exec "$vmid" -- dpkg -l 2>/dev/null | grep -q "^ii.*docker-ce"; then
        name=$(pct list | awk -v id="$vmid" '$1==id {print $3}')
        echo "[$vmid - $name] Has Docker CE, will migrate"

        if [[ "$DRY_RUN" == "false" ]]; then
            echo "  Stopping Docker containers..."
            pct exec "$vmid" -- bash -c 'cd /root && docker compose down 2>/dev/null || docker-compose down 2>/dev/null || true'
            pct exec "$vmid" -- systemctl stop docker 2>/dev/null || true

            echo "  Removing Docker CE..."
            pct exec "$vmid" -- bash -c 'apt-mark unhold docker-ce docker-ce-cli containerd.io 2>/dev/null || true'
            pct exec "$vmid" -- bash -c 'DEBIAN_FRONTEND=noninteractive apt-get remove --purge -y docker-ce docker-ce-cli docker-ce-rootless-extras docker-buildx-plugin docker-compose-plugin containerd.io 2>/dev/null || true'
            pct exec "$vmid" -- bash -c 'rm -f /etc/apt/sources.list.d/docker.list /etc/apt/keyrings/docker.asc /etc/apt/keyrings/docker.gpg'

            echo "  Cleaning up..."
            pct exec "$vmid" -- bash -c 'apt-get autoremove -y 2>/dev/null || true'
            pct exec "$vmid" -- bash -c 'apt-get autoclean 2>/dev/null || true'

            echo "  Installing Debian docker.io..."
            pct exec "$vmid" -- bash -c 'apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y docker.io docker-compose'
            pct exec "$vmid" -- systemctl enable docker
            pct exec "$vmid" -- systemctl start docker

            echo "  Restarting compose services..."
            pct exec "$vmid" -- bash -c 'cd /root && docker-compose up -d 2>/dev/null || true'

            echo "  ✓ Done"
        else
            echo "  [DRY RUN] Would migrate this container"
        fi
        echo ""
    elif pct exec "$vmid" -- dpkg -l 2>/dev/null | grep -q "^ii.*docker.io"; then
        name=$(pct list | awk -v id="$vmid" '$1==id {print $3}')
        echo "[$vmid - $name] Already has Debian docker.io, skipping"
        echo ""
    fi
done

echo "=== Migration Complete ==="
