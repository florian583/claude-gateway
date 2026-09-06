#!/bin/sh
set -eu
cd "$(dirname "$0")"
swift build -c release
output="../bin/Claude Gateway Menu.app"
mkdir -p "$output/Contents/MacOS"
install -m 755 .build/release/GatewayMenu "$output/Contents/MacOS/GatewayMenu"
install -m 644 Info.plist "$output/Contents/Info.plist"
codesign --force --sign - "$output"
printf '%s\n' "Built bin/Claude Gateway Menu.app (local ad-hoc signature; not notarized)."
