#!/bin/bash
# Installs qq on all nodes of the sokar cluster.
# Script version: 0.2.0

set -euo pipefail

# -----------------------
# Configuration
# -----------------------

# qq version to install
QQ_VERSION="v0.2.0"

# GitHub release assets
INSTALL_SCRIPT_URL="https://github.com/Ladme/qq/releases/download/${QQ_VERSION}/qq-install.sh"
RELEASE_URL="https://github.com/Ladme/qq/releases/download/${QQ_VERSION}/qq-release.tar.gz"

# list of target home directories
TARGET_HOMES=(
    "${HOME}"
)

# -----------------------
# Main logic
# -----------------------

TMP_INSTALLER="$(mktemp)"

echo "INFO    [qq sokar installer] Downloading qq installer from ${INSTALL_SCRIPT_URL}..."
curl -fsSL -o "$TMP_INSTALLER" "$INSTALL_SCRIPT_URL"
chmod +x "$TMP_INSTALLER"

echo "INFO    [qq sokar installer] Installing qq ${QQ_VERSION} from ${RELEASE_URL}"

for HOME_DIR in "${TARGET_HOMES[@]}"; do
    echo "--------------------------------------------"
    echo "INFO    [qq sokar installer] Installing qq into $HOME_DIR ..."
    if [ -d "$HOME_DIR" ]; then
        "$TMP_INSTALLER" "$HOME_DIR" "$RELEASE_URL"
    else
        echo "WARN    [qq sokar installer] Skipping $HOME_DIR (directory not found)"
    fi
done

echo "--------------------------------------------"
echo "INFO    [qq sokar installer] qq installation completed for all target home directories."
echo "INFO    [qq sokar installer] Run 'source ${HOME}/.bashrc' to make qq available on the current machine."

# Cleanup
rm -f "$TMP_INSTALLER"
