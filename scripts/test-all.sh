#!/usr/bin/env bash
# 一键门禁（M1 全链路 E2E 体系）：
#   格式 → clippy → 单测/集成（默认 features）→ all-features → 覆盖率（可选）。
#
# 用法：bash scripts/test-all.sh
set -euo pipefail
cd "$(dirname "$0")/.."

echo "==> cargo fmt --all -- --check"
cargo fmt --all -- --check

# 注意：vendored ctp2rs 已通过根 Cargo.toml 的 `[workspace].exclude` 移出成员，
# 因此 `--all-features` 只作用于本仓库 crate，不会同时打开 ctp2rs 互斥的
# `v1alpha1` / `v1alpha2`。
echo "==> cargo clippy --workspace --all-targets --all-features -- -D warnings"
cargo clippy --workspace --all-targets --all-features -- -D warnings

echo "==> cargo test --workspace"
cargo test --workspace

echo "==> cargo test --workspace --all-features"
cargo test --workspace --all-features

# 覆盖率：llvm-cov 属于可选工具（CI 镜像未装时不应阻断门禁）。
if command -v cargo-llvm-cov >/dev/null 2>&1; then
  echo "==> cargo llvm-cov --workspace --summary-only"
  cargo llvm-cov --workspace --summary-only
else
  echo "==> SKIP: cargo-llvm-cov 未安装；安装后重跑可生成覆盖率摘要"
fi

echo "==> 全部门禁通过"
