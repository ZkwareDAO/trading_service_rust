#!/usr/bin/env bash
#
# trading-service 卸载脚本（与 deploy.sh 的 /opt 布局对应）。
#
# 用法：
#   sudo ./deploy/uninstall.sh           # 卸载服务并删除安装目录（有确认提示）
#   sudo ./deploy/uninstall.sh --purge   # 跳过确认，连数据（账户/密钥/账本）一起删
#
# 默认行为：停止并禁用服务、删除 systemd 单元与管理入口软链、删除 /opt
# 安装目录中的 bin/conf/logs，但**保留 data/**（账户凭证与账本，防止误删）。
# --purge 才会连 data/ 一起删除。
#
set -euo pipefail

PROJECT_NAME=trading-service
OPT_ROOT="/opt/trading_service_rust"
DATA_DIR="${OPT_ROOT}/data"
LOG_DIR="${OPT_ROOT}/logs"
CONF_DIR="${OPT_ROOT}/conf"
SERVICE_DST=/etc/systemd/system/${PROJECT_NAME}.service
MANAGE_LINK=/usr/local/bin/${PROJECT_NAME}

PURGE=false
[ "${1:-}" = "--purge" ] && PURGE=true

if [ "$(id -u)" -ne 0 ]; then
  echo "错误：请用 sudo 运行：sudo ./deploy/uninstall.sh" >&2
  exit 1
fi

echo "==> 停止并禁用服务"
systemctl stop ${PROJECT_NAME} 2>/dev/null || true
systemctl disable ${PROJECT_NAME} 2>/dev/null || true
rm -f "$SERVICE_DST"
systemctl daemon-reload
systemctl reset-failed ${PROJECT_NAME} 2>/dev/null || true

echo "==> 删除管理入口软链"
rm -f "$MANAGE_LINK"

if $PURGE; then
  echo "==> --purge：删除整个安装目录（含 data/ 账户凭证与账本）"
  echo
  echo "  即将删除：$OPT_ROOT（含 account.csv / account.key / south.key / 全部账本）"
  echo "  account.key 一旦删除，已加密的 api_secret 将无法解密！"
  read -r -p "确认删除？输入 yes 继续：" ans
  [ "$ans" = "yes" ] || { echo "已取消。"; exit 1; }
  rm -rf "$OPT_ROOT"
else
  echo "==> 删除 bin/conf/logs（保留 $DATA_DIR）"
  rm -rf "${OPT_ROOT}/bin" "$CONF_DIR" "$LOG_DIR"
  if [ -d "$DATA_DIR" ]; then
    echo
    echo "已保留数据目录 $DATA_DIR（account.csv、密钥与账本）。"
    echo "如确认不再需要，可手动删除：sudo rm -rf $OPT_ROOT"
  fi
fi

echo
echo "卸载完成。"
