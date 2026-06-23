#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

HISTORIOUS_REPO_ROOT="$repo_root" cargo run -p xtask -- release "$@"
