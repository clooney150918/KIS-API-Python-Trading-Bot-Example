#!/usr/bin/env bash
set -euo pipefail

ROOT="$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
IMAGE="soxl-jinho-offline-tests:task0"
BASE_IMAGE="bots-soxl-trading-jinho@sha256:9b428cb5148282aa2ab5c9c3bd289315bd8a963c576c6a38b593a62cbd96f4ac"
BASE_ID="sha256:9b428cb5148282aa2ab5c9c3bd289315bd8a963c576c6a38b593a62cbd96f4ac"
REBUILD=false

if [[ "${1:-}" == "--rebuild" ]]; then
    REBUILD=true
    shift
fi
if [[ "${1:-}" == "--" ]]; then
    shift
fi

empty_env="$(mktemp)"
build_context=""
cleanup() {
    rm -f -- "$empty_env"
    if [[ -n "$build_context" ]]; then
        rm -rf -- "$build_context"
    fi
}
trap cleanup EXIT
chmod 0444 "$empty_env"

run_args=(
    run
    --rm
    --network=none
    --read-only
    --cap-drop=ALL
    --security-opt=no-new-privileges
    --user=65532:65532
    --pids-limit=128
    --tmpfs=/tmp:rw,noexec,nosuid,nodev,mode=1777,size=64m
    --mount="type=bind,src=$ROOT,dst=/app,readonly"
    --mount="type=bind,src=$empty_env,dst=/app/.env,readonly"
    --env=OPERATOR_HALT=true
    --env=LIVE_ARMED=false
    --env=SHADOW_ONLY=true
    --env=PYTHONDONTWRITEBYTECODE=1
    --env=HOME=/tmp/home
    --workdir=/app
)

refuse_unsafe() {
    printf 'refusing unsafe test execution: required option missing: %s\n' "$1" >&2
    exit 64
}

require_exact() {
    local required="$1" option
    for option in "${run_args[@]}"; do
        [[ "$option" == "$required" ]] && return 0
    done
    refuse_unsafe "$required"
}

for required in \
    --network=none \
    --read-only \
    --cap-drop=ALL \
    --security-opt=no-new-privileges \
    --user=65532:65532 \
    --tmpfs=/tmp:rw,noexec,nosuid,nodev,mode=1777,size=64m \
    "--mount=type=bind,src=$ROOT,dst=/app,readonly" \
    "--mount=type=bind,src=$empty_env,dst=/app/.env,readonly" \
    --env=PYTHONDONTWRITEBYTECODE=1; do
    require_exact "$required"
done

actual_base_id="$(docker image inspect --format '{{.Id}}' "$BASE_IMAGE" 2>/dev/null || true)"
[[ "$actual_base_id" == "$BASE_ID" ]] || {
    printf 'pinned local base image is unavailable or mismatched\n' >&2
    exit 65
}

harness_hash="$(sha256sum "$ROOT/Dockerfile.test" "$ROOT/requirements-test.txt" | sha256sum | cut -d' ' -f1)"

if [[ "$REBUILD" == true ]]; then
    build_context="$(mktemp -d)"
    install -m 0644 "$ROOT/Dockerfile.test" "$build_context/Dockerfile"
    install -m 0644 "$ROOT/requirements-test.txt" "$build_context/requirements-test.txt"
    docker build \
        --pull=false \
        --label "org.hermes.test-harness.sha256=$harness_hash" \
        --tag "$IMAGE" \
        "$build_context"
fi

image_hash="$(docker image inspect --format '{{index .Config.Labels "org.hermes.test-harness.sha256"}}' "$IMAGE" 2>/dev/null || true)"
[[ "$image_hash" == "$harness_hash" ]] || {
    printf 'verified test image is missing or stale; run %s --rebuild\n' "$0" >&2
    exit 66
}

docker "${run_args[@]}" "$IMAGE" \
    python -m pytest -q -p no:cacheprovider "$@"
