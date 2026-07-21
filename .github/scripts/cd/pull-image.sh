#!/usr/bin/env bash
# Pulls a signed image into the target server's Docker cache. This script never
# creates, restarts, or removes containers.
set -Eeuo pipefail

require_var() {
  local name="$1"
  [[ -n "${!name:-}" ]] || {
    echo "Required environment variable '$name' is not set." >&2
    exit 1
  }
}

require_var REGISTRY
require_var IMAGE_NAME
require_var VERSION
require_var GHCR_USERNAME
require_var GHCR_TOKEN
require_var COSIGN_CERT_IDENTITY_REGEXP
require_var COSIGN_CERT_OIDC_ISSUER

image_name="${IMAGE_NAME,,}"
printf '%s' "$GHCR_TOKEN" |
  docker login "$REGISTRY" -u "$GHCR_USERNAME" --password-stdin

if [[ -n "${REQUESTED_IMAGE:-}" ]]; then
  image="$REQUESTED_IMAGE"
else
  image_tag="${REGISTRY}/${image_name}:${VERSION}"
  docker pull "$image_tag"
  image="$(docker image inspect --format '{{index .RepoDigests 0}}' "$image_tag")"
  [[ "$image" == *"@sha256:"* ]] || {
    echo "Could not resolve ${image_tag} to an immutable digest." >&2
    exit 1
  }
fi

cosign verify \
  --certificate-identity-regexp "$COSIGN_CERT_IDENTITY_REGEXP" \
  --certificate-oidc-issuer "$COSIGN_CERT_OIDC_ISSUER" \
  "$image" >/dev/null
docker pull "$image"
docker image inspect "$image" \
  --format 'Pulled immutable image: {{index .RepoDigests 0}}'
