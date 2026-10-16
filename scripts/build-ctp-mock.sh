#!/usr/bin/env bash
# 构建 ctp-mock：两个 ABI 替身 .so + 控制面 CLI + 黑盒常驻进程。
#
# 产物（third_party/ctp-mock/target/<profile>/）：
#   libctpmock_td.so   交易前置替身（trader_so 指向它）
#   libctpmock_md.so   行情前置替身（md_so 指向它）
#   ctp-mock-ctl       控制面 CLI
#   examples/headless_runner  联调用常驻进程（dlopen 两个 .so）
#
# 用法：scripts/build-ctp-mock.sh [--release]
set -euo pipefail

PROFILE="debug"
CARGO_ARGS=()
if [[ "${1:-}" == "--release" ]]; then
  PROFILE="release"
  CARGO_ARGS+=(--release)
fi

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WS="$REPO_ROOT/third_party/ctp-mock"

# ctp2rs build.rs 经 bindgen 解析官方头文件，需要 libclang。
if [[ -z "${LIBCLANG_PATH:-}" ]]; then
  if [[ -d /usr/lib/llvm-21/lib ]]; then export LIBCLANG_PATH=/usr/lib/llvm-21/lib
  elif [[ -e /usr/lib/x86_64-linux-gnu/libclang-21.so.21 ]]; then export LIBCLANG_PATH=/usr/lib/x86_64-linux-gnu
  fi
fi
echo "LIBCLANG_PATH=${LIBCLANG_PATH:-<from toolchain default>}"

cd "$WS"
# 独立嵌套 workspace（自带 target），与主 workspace 解耦。
cargo build --workspace "${CARGO_ARGS[@]}"
cargo build --example headless_runner "${CARGO_ARGS[@]}"

OUT="$WS/target/$PROFILE"
echo
echo "构建完成（profile=$PROFILE）："
for f in libctpmock_td.so libctpmock_md.so ctp-mock-ctl; do
  if [[ -e "$OUT/$f" ]]; then echo "  $OUT/$f"; fi
done
[[ -e "$OUT/examples/headless_runner" ]] && echo "  $OUT/examples/headless_runner"
