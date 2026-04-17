#!/bin/bash
set -euo pipefail

if ! command -v module >/dev/null 2>&1; then
  if [[ -f /etc/profile ]]; then
    # Ensure the module function is available in non-login shells.
    source /etc/profile
  fi
fi

module use --append /pl/active/koala/software/lmod-files
module load uv
uv -v

_user_name=${USER:-${LOGNAME:-unknown}}
export UV_BASE=${UV_BASE:-/projects/${_user_name}/uv}
export UV_CACHE_DIR=${UV_CACHE_DIR:-$UV_BASE/cache}
export UV_PYTHON_INSTALL_DIR=${UV_PYTHON_INSTALL_DIR:-$UV_BASE/python}
export UV_TOOL_DIR=${UV_TOOL_DIR:-$UV_BASE/tools}

mkdir -p "$UV_CACHE_DIR" "$UV_PYTHON_INSTALL_DIR" "$UV_TOOL_DIR"
