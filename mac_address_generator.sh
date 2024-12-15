#!/bin/bash

# Function to generate a MAC address based on the hostname and virtualisation platform
# Inspired by Alain Kelder http://giantdorks.org/alain/how-to-generate-a-unique-mac-address/
my_mac_generator()
{
  # Prompt for hostname
  echo "Enter hostname:"
  read -p "Hostname: " hostname

  # Validate hostname
  if [[ ! "$hostname" =~ ^[A-Za-z0-9-]+$ ]]; then
    echo "Invalid hostname. Only A-Z, a-z, 0-9, and '-' are allowed."
    exit 1
  fi

  echo "Please select virtualisation platform:"
  echo "1. Proxmox"
  echo "2. Xen/LXC"
  echo "3. VMWare"
  echo "4. VirtualBox"
  echo "5. KVM"
  read -p "Enter choice [1-5]: " choice

  case $choice in
    1) OUI="bc:24:11" ;;
    2) OUI="00:16:3e" ;;
    3) OUI="00:50:56" ;;
    4) OUI="08:00:27" ;;
    5) OUI="52:54:00" ;;
    *) echo "Invalid choice"; exit 1 ;;
  esac

  RAND=$(echo "$hostname" | md5sum | sed 's/\(..\)\(..\)\(..\).*/\1:\2:\3/')
  echo "$OUI:$RAND"
}

my_mac_generator