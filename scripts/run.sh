#!/usr/bin/env bash
# Прогнать бенч и обновить графики.
#
# Зависимости: rustup/cargo, python3 + matplotlib.
#   pip install --user matplotlib   # или:  pacman -S python-matplotlib

set -euo pipefail

cd "$(dirname "$0")/.."

PRODUCERS="${PRODUCERS:-4}"
SIZES="${SIZES:-small medium large huge}"
OUT_DIR="${OUT_DIR:-out}"

mkdir -p "$OUT_DIR"

echo ">>> cargo build --release"
cargo build --release

echo ">>> ./target/release/bench --producers $PRODUCERS --sizes $SIZES --out $OUT_DIR/bench.json"
./target/release/bench \
    --producers "$PRODUCERS" \
    --sizes $SIZES \
    --out "$OUT_DIR/bench.json"

echo ">>> python3 scripts/plot.py $OUT_DIR/bench.json $OUT_DIR/"
python3 scripts/plot.py "$OUT_DIR/bench.json" "$OUT_DIR/"

echo
echo "Done. Results:"
echo "  $OUT_DIR/bench.json"
ls -1 "$OUT_DIR"/*.png 2>/dev/null | sed 's/^/  /' || true
