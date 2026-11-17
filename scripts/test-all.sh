#!/usr/bin/env bash
# 一键门禁（M1 自动化测试体系）：
#   格式 → clippy → 单测/集成（默认 features）→ all-features → 覆盖率（可选）。
#
# 测试资产分层（workspace 自动覆盖，以下为定位问题时的聚焦命令）：
#   cargo test -p trading-service --test e2e           # 跨模块业务链 E2E（21 用例）
#   cargo test -p trading-service --test blackbox      # 进程级黑盒 smoke（4 用例）
#   cargo test -p trading-exchange --test conformance   # 交易所接口契约（后端无关）
#   cargo test -p trading-service --test api            # REST 接口级（33 用例，ctp 时 34）
#   cargo test -p trading-service --test south_ws       # 南向 WS 协议契约
#   cargo test -p trading-service --test persistence    # 持久化往返
#   cargo test -p trading-service --test properties     # L1 服务层不变量
#   cargo test -p trading-models  --test properties     # L1 领域模型不变量
#   cargo test -p trading-sim                            # 模拟所自测
#
# 用法：bash scripts/test-all.sh
#
# 资源约束（见 CLAUDE.md）：同一时刻只跑一个 cargo 进程，并限制构建并发与
# 测试线程，避免本机 OOM/IO 阻塞。需要更高并发时显式覆盖环境变量即可。
set -euo pipefail
cd "$(dirname "$0")/.."

export CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-2}"
TEST_THREADS="${TEST_THREADS:-2}"

echo "==> cargo fmt --all -- --check"
cargo fmt --all -- --check

# 注意：vendored ctp2rs 已通过根 Cargo.toml 的 `[workspace].exclude` 移出成员，
# 因此 `--all-features` 只作用于本仓库 crate，不会同时打开 ctp2rs 互斥的
# `v1alpha1` / `v1alpha2`。
echo "==> cargo clippy --workspace --all-targets --all-features -- -D warnings"
cargo clippy --workspace --all-targets --all-features -- -D warnings

echo "==> cargo test --workspace (--test-threads=$TEST_THREADS)"
cargo test --workspace -- --test-threads="$TEST_THREADS"

echo "==> cargo test --workspace --all-features (--test-threads=$TEST_THREADS)"
cargo test --workspace --all-features -- --test-threads="$TEST_THREADS"

# 覆盖率：llvm-cov 属于可选工具（CI 镜像未装时不应阻断门禁）。
if command -v cargo-llvm-cov >/dev/null 2>&1; then
  echo "==> cargo llvm-cov --workspace --summary-only (--test-threads=$TEST_THREADS)"
  cargo llvm-cov --workspace --summary-only -- --test-threads="$TEST_THREADS"
else
  echo "==> SKIP: cargo-llvm-cov 未安装；安装后重跑可生成覆盖率摘要"
fi

echo "==> 全部门禁通过"
