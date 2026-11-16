#!/usr/bin/env bash
# Cargo audit con la lista canonica de advisories ignorados en deny.toml.
# Uso local: ./scripts/security-audit.sh --db "$HOME/.cargo/advisory-db/advisory-db-*" --no-fetch
set -euo pipefail

cd "$(dirname "$0")/.."

mapfile -t ignores < <(python3 - <<'PY'
import tomllib

with open("deny.toml", "rb") as f:
    print("\n".join(tomllib.load(f)["advisories"]["ignore"]))
PY
)

args=()
for id in "${ignores[@]}"; do
  args+=(--ignore "$id")
done

exec cargo audit "${args[@]}" "$@"
