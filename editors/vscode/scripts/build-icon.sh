#!/usr/bin/env bash
# Renders the Marketplace icon from the repository logo. The Marketplace
# rejects SVG icons and requires a PNG of at least 128x128.
#
# Requires rsvg-convert (librsvg). Without it installed, run:
#   nix-shell -p librsvg --run ./scripts/build-icon.sh
set -euo pipefail

cd "$(dirname "$0")/.."
rsvg-convert -w 256 -h 256 ../../docs/assets/logo.svg -o media/icon.png
