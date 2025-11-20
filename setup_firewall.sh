############## LLM Generated Code Begins ##############
#!/bin/bash
# Script to add NamedServer and StorageServer to macOS firewall exceptions
# Run this after recompiling: sudo ./setup_firewall.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "Adding firewall exceptions for NamedServer and StorageServer..."

sudo /usr/libexec/ApplicationFirewall/socketfilterfw --add "$SCRIPT_DIR/NamedServer"
sudo /usr/libexec/ApplicationFirewall/socketfilterfw --unblockapp "$SCRIPT_DIR/NamedServer"

sudo /usr/libexec/ApplicationFirewall/socketfilterfw --add "$SCRIPT_DIR/StorageServer"
sudo /usr/libexec/ApplicationFirewall/socketfilterfw --unblockapp "$SCRIPT_DIR/StorageServer"

sudo /usr/libexec/ApplicationFirewall/socketfilterfw --add "$SCRIPT_DIR/Client"
sudo /usr/libexec/ApplicationFirewall/socketfilterfw --unblockapp "$SCRIPT_DIR/Client"

echo "✅ Firewall exceptions added successfully!"
############## LLM Generated Code Ends ################