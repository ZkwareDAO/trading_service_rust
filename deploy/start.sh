#!/usr/bin/env bash
#
# trading-service 启动脚本。
#
# deploy.sh 只负责安装（不 enable、不启动）；完成账户与 [ctp] 配置后执行本脚本。
#
# 用法：
#   sudo /opt/trading_service_rust/bin/start.sh
#
# 行为：daemon-reload -> enable（开机自启）-> restart（首次即启动；已运行则应用新配置）。
# 账户变更不需要本脚本，执行 `trading-service reload` 即可；[ctp] 变更后重跑本脚本。
#
set -euo pipefail

SERVICE=trading-service
OPT_ROOT=/opt/trading_service_rust
CONF="$OPT_ROOT/conf/venue.toml"
DATA="$OPT_ROOT/data"
LOG="$OPT_ROOT/logs/trading-service.log"
UNIT="/etc/systemd/system/${SERVICE}.service"

if [ "$(id -u)" -ne 0 ]; then
  echo "错误：请用 sudo 运行：sudo $0" >&2
  exit 1
fi
if [ ! -f "$UNIT" ]; then
  echo "错误：未找到 $UNIT，请先执行 deploy/deploy.sh 完成安装。" >&2
  exit 1
fi
if [ ! -x "$OPT_ROOT/bin/trading-service" ]; then
  echo "错误：未找到 $OPT_ROOT/bin/trading-service，请先执行 deploy/deploy.sh。" >&2
  exit 1
fi

# 软检查：只提醒、不阻断（md_front 为空时 CTP 行情线程不会启动）。
if [ -f "$CONF" ]; then
  if ! grep -qE '^[[:space:]]*md_front[[:space:]]*=[[:space:]]*"[^"]+"' "$CONF"; then
    echo "警告：$CONF 的 [ctp] md_front 缺失或为空，CTP 行情线程不会启动。" >&2
  fi
  if ! grep -qE '^[[:space:]]*md_account[[:space:]]*=[[:space:]]*"[^"]+"' "$CONF"; then
    echo "警告：$CONF 的 [ctp] md_account 缺失或为空，CTP 行情线程不会启动。" >&2
  fi
fi
if [ -f "$DATA/account.csv" ] && ! tail -n +2 "$DATA/account.csv" | grep -q ',ctp,'; then
  echo "警告：$DATA/account.csv 中还没有 CTP 账户（exchange=ctp）。" >&2
fi

systemctl daemon-reload
systemctl enable "$SERVICE"
if ! systemctl restart "$SERVICE"; then
  echo "启动失败，查看日志：$LOG" >&2
  systemctl --no-pager --full status "$SERVICE" | head -20 || true
  exit 1
fi
sleep 2
systemctl --no-pager --full status "$SERVICE" | head -20 || true

echo
echo "已启动 $SERVICE。"
echo "  日志：tail -f $LOG"
echo "  账户变更：trading-service reload（无需重启）"
echo "  [ctp] 变更：sudo $OPT_ROOT/bin/start.sh（本脚本会 restart）"
