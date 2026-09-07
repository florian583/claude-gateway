#!/usr/bin/env bash
set -euo pipefail

target_os=${1:?usage: package.sh darwin\|linux arm64\|amd64}
target_arch=${2:?usage: package.sh darwin\|linux arm64\|amd64}
case "$target_os/$target_arch" in
  darwin/arm64|darwin/amd64|linux/arm64|linux/amd64) ;;
  *) echo "unsupported target" >&2; exit 1 ;;
esac

cd "$(dirname "$0")/.."
stage=$(mktemp -d)
trap 'rm -r -- "$stage"' EXIT
name="claude-proxy-$target_os-$target_arch"
mkdir -p "$stage/$name" dist
source_hash=$(shasum -a 256 *.go go.mod | shasum -a 256 | cut -d ' ' -f 1)
CGO_ENABLED=0 GOOS="$target_os" GOARCH="$target_arch" go build \
  -trimpath -ldflags "-s -w -X main.buildSourceHash=$source_hash" \
  -o "$stage/$name/claude-proxy" .
# Explicit allowlist: never package a checkout, private config, or runtime state.
cp README.md "$stage/$name/"
while IFS= read -r -d '' file; do
  mkdir -p "$stage/$name/$(dirname "$file")"
  cp "$file" "$stage/$name/$file"
done < <(git ls-files -z docs examples)
tar -czf "dist/$name.tar.gz" -C "$stage" "$name"
(cd dist && shasum -a 256 "$name.tar.gz" > "$name.tar.gz.sha256")
echo "dist/$name.tar.gz"
