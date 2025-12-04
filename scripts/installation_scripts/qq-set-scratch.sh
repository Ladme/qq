#!/bin/bash
# Sets up .bashrc to sync qq from a specified directory to /scratch/${USER}
# and add both locations to PATH (with /scratch/${USER}/qq prioritized).
# Script version: 0.3.1

set -euo pipefail

# -------------------
# Usage & parameters
# -------------------
if [[ $# -ne 2 ]]; then
    echo "Usage: $0 <target_home_directory> <path_to_qq>"
    exit 1
fi

TARGET_HOME="$1"
PATH_TO_QQ="$2"

if [[ ! -d "$TARGET_HOME" ]]; then
    echo "ERROR   [qq setup] Target home directory '$TARGET_HOME' does not exist."
    exit 1
fi

if [[ ! -d "$PATH_TO_QQ" ]]; then
    echo "ERROR   [qq setup] qq directory '$PATH_TO_QQ' does not exist."
    exit 1
fi

# -------------------
# Variables
# -------------------
BASHRC="${TARGET_HOME}/.bashrc"

# detect version if VERSION file exists
if [[ -f "$PATH_TO_QQ/VERSION" ]]; then
    VERSION=$(cat "$PATH_TO_QQ/VERSION")
else
    echo "ERROR   [$TARGET_HOME] Could not detect qq version. Please, report this issue."
    exit 1
fi

BLOCK_START="# >>> This block is managed by qq (v${VERSION}) >>>"
BLOCK_END="# <<< This block is managed by qq (v${VERSION}) <<<"

# -----------------------------
# Update .bashrc in target home
# -----------------------------
touch "$BASHRC"

generate_qq_block() {
    cat <<EOF
$BLOCK_START
# This makes qq available from the home directory.
if [ -d "$PATH_TO_QQ" ] && [[ ":\${PATH}:" != *":$PATH_TO_QQ:"* ]]; then
    export PATH="\${PATH}:$PATH_TO_QQ"
fi
# This makes qq available on scratch (where it is faster).
if [ -d "$PATH_TO_QQ" ]; then
    rsync -a --delete --quiet "$PATH_TO_QQ/" "/scratch/\${USER}/qq/" 2>/dev/null || true
    if [ -d "/scratch/\${USER}/qq" ] && [[ ":\${PATH}:" != *":/scratch/\${USER}/qq:"* ]]; then
        export PATH="/scratch/\${USER}/qq:\${PATH}"
    fi
fi
# This makes the qq cd command work.
qq() {
    if [[ "\$1" == "cd" ]]; then
        for arg in "\$@"; do
            if [[ "\$arg" == "--help" || "\$arg" == "-h" ]]; then
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

update_bashrc_block() {
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

update_bashrc_block "$BASHRC"

echo "INFO    [$TARGET_HOME] qq PATH and cd function configured for $PATH_TO_QQ (version $VERSION)."