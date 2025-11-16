#!/bin/bash
# Installs qq from a GitHub release into a specific home directory
# and updates that home directory's specified rc file accordingly.
# Script version: 0.1.0-experimental

set -euo pipefail

# -------------------
# Usage & parameters
# -------------------
if [[ $# -ne 3 ]]; then
    echo "Usage: $0 <target_home_directory> <rc_file> <github_release_url>"
    exit 1
fi

TARGET_HOME="$1"
RC_NAME="$2"
GITHUB_URL="$3"


if [[ ! -d "$TARGET_HOME" ]]; then
    echo "ERROR   [qq custom-rc installer] Target home directory '$TARGET_HOME' does not exist."
    exit 1
fi

# -------------------
# Variables
# -------------------
RC="${TARGET_HOME}/${RC_NAME}"
PATH_TO_QQ="${TARGET_HOME}/qq"
TMPDIR=$(mktemp -d)
ARCHIVE_PATH="${TMPDIR}/qq-release.tar.gz"

echo "INFO    [$TARGET_HOME] Downloading qq from GitHub release..."
curl -L -o "$ARCHIVE_PATH" "$GITHUB_URL"

echo "INFO    [$TARGET_HOME] Extracting qq..."
tar -xzf "$ARCHIVE_PATH" -C "$TMPDIR"

# find extracted qq directory
EXTRACTED_DIR=$(find "$TMPDIR" -maxdepth 1 -type d -name "qq*" ! -path "$TMPDIR" | head -n 1)
if [[ -z "$EXTRACTED_DIR" ]]; then
    echo "ERROR   [$TARGET_HOME] Could not find extracted qq directory."
    exit 1
fi

# detect version if VERSION file exists
if [[ -f "$EXTRACTED_DIR/VERSION" ]]; then
    VERSION=$(cat "$EXTRACTED_DIR/VERSION")
else
    echo "ERROR   [$TARGET_HOME] Could not detect qq version. Please, report this issue."
    exit 1
fi

BLOCK_START="# >>> This block is managed by qq (v${VERSION}) >>>"
BLOCK_END="# <<< This block is managed by qq (v${VERSION}) <<<"

# ---------------------------
# Install qq into target home
# ---------------------------
if [ -d "$PATH_TO_QQ" ]; then
    echo "INFO    [$TARGET_HOME] qq already present."

    if [ -f "$PATH_TO_QQ/VERSION" ]; then
        INSTALLED_VERSION=$(cat "$PATH_TO_QQ/VERSION")
        if [ "$INSTALLED_VERSION" != "$VERSION" ]; then
            echo "INFO    [$TARGET_HOME] Version mismatch (installed: $INSTALLED_VERSION, new: $VERSION). Updating qq..."
            rm -rf "$PATH_TO_QQ"
            cp -r "$EXTRACTED_DIR" "$PATH_TO_QQ"
            echo "INFO    [$TARGET_HOME] qq updated to version $VERSION."
        else
            echo "INFO    [$TARGET_HOME] qq is up to date (version $VERSION)."
        fi
    else
        echo "INFO    [$TARGET_HOME] VERSION file missing. Reinstalling qq..."
        rm -rf "$PATH_TO_QQ"
        cp -r "$EXTRACTED_DIR" "$PATH_TO_QQ"
        echo "INFO    [$TARGET_HOME] qq installed with version $VERSION."
    fi
else
    cp -r "$EXTRACTED_DIR" "$PATH_TO_QQ"
    echo "INFO    [$TARGET_HOME] Installed qq (version $VERSION)."
fi

# -----------------------------
# Update rc in target home
# -----------------------------
touch "$RC"

generate_qq_block() {
    cat <<EOF
$BLOCK_START
# This makes qq available for you on any computer using this directory as its HOME.
if [[ ":\$PATH:" != *":$PATH_TO_QQ:"* ]]; then
    export PATH="\$PATH:$PATH_TO_QQ"
fi
# This makes the qq cd command work.
qq() {
    if [[ "\$1" == "cd" ]]; then
        for arg in "\$@"; do
            if [[ "\$arg" == "--help" ]]; then
                command qq "\$@"
                return
            fi
        done
        target_dir="\$(command qq cd "\${@:2}")"
        cd "\$target_dir" || return
    else
        command qq "\$@"
    fi
}
# This makes qq autocomplete work.
eval "\$(_QQ_COMPLETE=bash_source qq)"
$BLOCK_END
EOF
}

update_rc_block() {
    local file="$1"
    local start_re="^# >>> This block is managed by qq"
    local end_re="^# <<< This block is managed by qq"

    if grep -qE "$start_re" "$file"; then
        echo "INFO    [$TARGET_HOME] Found existing qq-managed block in $file. Replacing..."
        awk -v start_re="$start_re" -v end_re="$end_re" '
            BEGIN { in_block=0 }
            $0 ~ start_re { in_block=1; next }
            $0 ~ end_re { in_block=0; next }
            in_block == 0 { print }
        ' "$file" > "$file.tmp"
        generate_qq_block >> "$file.tmp"
        mv "$file.tmp" "$file"
    else
        echo "INFO    [$TARGET_HOME] No qq-managed block found. Adding new block..."
        echo "" >> "$file"
        generate_qq_block >> "$file"
        echo "" >> "$file"
    fi
}

update_rc_block "$RC"

echo "INFO    [$TARGET_HOME] qq PATH and cd function ensured (version $VERSION)."

# Cleanup
rm -rf "$TMPDIR"