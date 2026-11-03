#!/usr/bin/env bash
#
# trading-service 一键部署脚本（/opt 一体化布局）。
#
# 用法：
#   sudo ./deploy/deploy.sh    # 一切装进 /opt/trading_service_rust
#
# （系统级 FHS 布局（/etc /var/lib /var/log）暂不提供，有需要再扩展 --fhs 模式。）
#
# /opt 模式布局（root 只在安装这一刻出现，后续全归普通用户）：
#   /opt/trading_service_rust/
#     ├── bin/      trading-service 二进制 + manage.sh 管理入口（软链到 /usr/local/bin）
#     ├── conf/     venue.toml（用户改这里）
#     ├── data/     account.csv / account.key / south.key / 按日 CSV 账本
#     ├── lib/ctp/  CTP vendored 动态库（thosttraderapi_se.so / thostmduserapi_se.so）
#     ├── logs/     trading-service.log
#     └── （源码仓库本身不复制；脚本只装运行所需文件）
#
# CTP：默认以 --features ctp 构建；首次安装自动把仓库 vendored 的 .so 装到 lib/ctp，
#      并向 conf/venue.toml 的 [ctp] 注入 trader_so/md_so（不触碰 md_front/md_account）。
#      前置依赖由用户预装：x86_64 Linux、Rust>=1.80、clang/libclang-dev、g++/make。
#
# 本脚本只安装：不 enable、不启动。完成账户与 [ctp] 配置后执行 bin/start.sh。
# 幂等：重复执行即升级——二进制重装、配置/数据/账户文件已存在则不动。
#
set -euo pipefail

PROJECT_NAME=trading-service
PROJECT_DIR_NAME=trading_service_rust
# /opt 模式安装根
OPT_ROOT="/opt/${PROJECT_DIR_NAME}"

# ——— 固定路径 ———
BIN_DST="${OPT_ROOT}/bin/${PROJECT_NAME}"
CONF_DIR="${OPT_ROOT}/conf"
DATA_DIR="${OPT_ROOT}/data"
LOG_DIR="${OPT_ROOT}/logs"
SERVICE_DST=/etc/systemd/system/${PROJECT_NAME}.service
MANAGE_LINK=/usr/local/bin/${PROJECT_NAME}   # 管理入口软链，方便敲命令

# ——— 前置检查 ———
if [ "$(id -u)" -ne 0 ]; then
  echo "错误：请用 sudo 运行：sudo ./deploy/deploy.sh" >&2
  exit 1
fi

# sudo 的 secure_path 会抹掉用户级 PATH（rustup 默认装在 ~/.cargo/bin，root 看不到）。
# 以 SUDO_USER 的 home 为线索找回 cargo；再找不到再报错。
RUN_USER="${SUDO_USER:-root}"
if ! command -v cargo >/dev/null 2>&1; then
  for cand in "/home/$RUN_USER/.cargo/bin" "$HOME/.cargo/bin"; do
    if [ -x "$cand/cargo" ]; then
      export PATH="$cand:$PATH"
      break
    fi
  done
fi
if ! command -v cargo >/dev/null 2>&1; then
  echo "错误：未找到 cargo，请先安装 Rust（https://rustup.rs，见 docs/部署文档.md §3.1.1）后重试。" >&2
  exit 1
fi

# 构建依赖（由用户预装；这里只 fail-fast，不自动安装）。
# CTP vendored 动态库仅支持 x86_64 Linux。
if [ "$(uname -m)" != "x86_64" ]; then
  echo "错误：CTP vendored 动态库仅支持 x86_64 Linux（当前 $(uname -m)）。" >&2
  exit 1
fi
if ! command -v clang >/dev/null 2>&1; then
  echo "错误：未找到 clang（ctp2rs build.rs 需要）。请安装 clang libclang-dev（见 docs/部署文档.md §3.1.1）。" >&2
  exit 1
fi
if ! command -v cc >/dev/null 2>&1 && ! command -v gcc >/dev/null 2>&1; then
  echo "错误：未找到 C 编译器（cc/gcc）。请安装 build-essential（见 docs/部署文档.md §3.1.1）。" >&2
  exit 1
fi
if ! (ldconfig -p 2>/dev/null | grep -q 'libclang') \
  && ! ls /usr/lib/llvm-*/lib/libclang.so* >/dev/null 2>&1; then
  echo "错误：未找到 libclang（ctp2rs build.rs 需要）。请安装 clang libclang-dev（见 docs/部署文档.md §3.1.1）。" >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
# CTP vendored 动态库（仓库内 v6.7.13 linux64）；运行期安装到 lib/ctp。
CTP_SO_SRC_DIR="$PROJECT_DIR/third_party/ctp2rs/ctp-dyn/api/ctp/v6.7.13/v6.7.13_20260225_api_traderapi_se_linux64"
CTP_LIB_DIR="${OPT_ROOT}/lib/ctp"
for so in thosttraderapi_se.so thostmduserapi_se.so; do
  if [ ! -f "$CTP_SO_SRC_DIR/$so" ]; then
    echo "错误：缺少 CTP 动态库 $CTP_SO_SRC_DIR/$so（请确认仓库 clone 完整）。" >&2
    exit 1
  fi
done
# ——— 部署源单元：一律以 deploy/trading-service.service 为准 ———
# .example 只是首次部署的种子模板：目录下没有 .service 时拷一份出来。
# 之后用户直接编辑 .service（监听地址、webhook、参数等全部以它为准），
# 重跑本脚本即生效；.service 已加入 .gitignore，不会污染 git status。
SERVICE_SRC="$SCRIPT_DIR/trading-service.service"
if [ ! -f "$SERVICE_SRC" ]; then
  cp "$SCRIPT_DIR/trading-service.service.example" "$SERVICE_SRC"
  echo "==> 已从 trading-service.service.example 复制出 $SERVICE_SRC"
  echo "    后续请编辑该文件自定义配置（如 --south-addr 监听地址），重跑部署即生效"
fi

for f in "$SERVICE_SRC" "$PROJECT_DIR/config/venue.toml" "$SCRIPT_DIR/account.example.csv" "$SCRIPT_DIR/start.sh"; do
  if [ ! -f "$f" ]; then
    echo "错误：缺少 $f，请在项目根目录下执行本脚本。" >&2
    exit 1
  fi
done

# ——— 1. 构建 release 二进制（核心服务 + account-tool 账户管理工具） ———
echo "==> cargo build --release --features ctp（首次编译可能需要数分钟）"
(cd "$PROJECT_DIR" && cargo build --release --features ctp -p trading-service \
  --bin trading-service --bin account-tool)
BIN_SRC="$PROJECT_DIR/target/release/trading-service"
TOOL_SRC="$PROJECT_DIR/target/release/account-tool"

# ——— 2. 建目录 ———
echo "==> 创建目录"
install -d -m 0755 -o "$RUN_USER" -g "$RUN_USER" \
  "${OPT_ROOT}/bin" "$CONF_DIR" "$DATA_DIR" "$LOG_DIR"

# ——— 3. 安装二进制与管理入口 ———
echo "==> 安装二进制"
install -m 0755 "$BIN_SRC" "$BIN_DST"
install -m 0755 "$TOOL_SRC" "${OPT_ROOT}/bin/account-tool"
# CTP 运行库：装到安装目录，避免依赖编译期绝对路径。
echo "==> 安装 CTP 动态库"
install -d -m 0755 -o "$RUN_USER" -g "$RUN_USER" "$CTP_LIB_DIR"
install -m 0755 -o "$RUN_USER" -g "$RUN_USER" \
  "$CTP_SO_SRC_DIR/thosttraderapi_se.so" "$CTP_LIB_DIR/"
install -m 0755 -o "$RUN_USER" -g "$RUN_USER" \
  "$CTP_SO_SRC_DIR/thostmduserapi_se.so" "$CTP_LIB_DIR/"
# manage.sh：普通用户的日常管理入口（状态/日志/重载账户/重启）。
cat > "${OPT_ROOT}/bin/manage.sh" << EOF
#!/usr/bin/env bash
# ${PROJECT_NAME} 管理入口（普通用户可直接执行）
# 导出数据目录，account-tool add-account 等工具据此取默认路径。
export TRADING_DATA_DIR="${DATA_DIR}"
export PATH="${OPT_ROOT}/bin:\$PATH"
case "\${1:-status}" in
  status)  systemctl --no-pager status ${PROJECT_NAME} ;;
  start)   sudo systemctl start ${PROJECT_NAME} ;;
  stop)    sudo systemctl stop ${PROJECT_NAME} ;;
  restart) sudo systemctl restart ${PROJECT_NAME} ;;
  log)     tail -f ${LOG_DIR}/${PROJECT_NAME}.log ;;
  reload)  curl -X POST http://127.0.0.1:9100/admin/accounts/reload ;;
  account) exec ${OPT_ROOT}/bin/account-tool add-account ;;
  conf)    echo "配置：${CONF_DIR}/venue.toml  数据：${DATA_DIR}" ;;
  *)       echo "用法：${PROJECT_NAME} {status|start|stop|restart|log|reload|account|conf}" ;;
esac
EOF
chmod 0755 "${OPT_ROOT}/bin/manage.sh"
chown "$RUN_USER:$RUN_USER" "${OPT_ROOT}/bin/manage.sh"
ln -sf "${OPT_ROOT}/bin/manage.sh" "$MANAGE_LINK"
# start.sh：deploy 本身不启动服务；配置完成后由用户执行本脚本。
install -m 0755 -o "$RUN_USER" -g "$RUN_USER" "$SCRIPT_DIR/start.sh" "${OPT_ROOT}/bin/start.sh"

# ——— 4. 配置文件（已存在则不动，保留用户调优） ———
# 首次安装时向 [ctp] 注入 .so 安装路径；不触碰 md_front/md_account。
inject_ctp_so_paths() {
  local conf="$1" libdir="$2" tmp
  tmp="$(mktemp)"
  awk -v trader="$libdir/thosttraderapi_se.so" -v md="$libdir/thostmduserapi_se.so" '
    BEGIN { in_ctp = 0; found = 0 }
    /^\[ctp\][[:space:]]*(#.*)?$/ {
      print
      print "trader_so = \"" trader "\""
      print "md_so = \"" md "\""
      in_ctp = 1; found = 1; next
    }
    /^\[/ { in_ctp = 0 }
    in_ctp && /^[[:space:]]*trader_so[[:space:]]*=/ { next }
    in_ctp && /^[[:space:]]*md_so[[:space:]]*=/ { next }
    { print }
    END {
      if (!found) {
        print ""
        print "[ctp]"
        print "trader_so = \"" trader "\""
        print "md_so = \"" md "\""
      }
    }
  ' "$conf" > "$tmp" && mv "$tmp" "$conf"
}

if [ ! -f "$CONF_DIR/venue.toml" ]; then
  install -m 0644 "$PROJECT_DIR/config/venue.toml" "$CONF_DIR/venue.toml"
  inject_ctp_so_paths "$CONF_DIR/venue.toml" "$CTP_LIB_DIR"
  chown "$RUN_USER:$RUN_USER" "$CONF_DIR/venue.toml" 2>/dev/null || true
  echo "    已安装 $CONF_DIR/venue.toml（已注入 [ctp] trader_so/md_so）"
  echo "    警告：conf/venue.toml 的 [ctp] md_front/md_account 可能来自仓库默认，"
  echo "          脚本不会修改；请确认属于本机柜台并手动替换。"
else
  echo "    $CONF_DIR/venue.toml 已存在，跳过（如需更新 [ctp] .so 路径请手动编辑）"
fi

# ——— 5. account.csv 初始化（不存在则只建表头，存在则不动） ———
ACCOUNT_CSV="$DATA_DIR/account.csv"
if [ ! -f "$ACCOUNT_CSV" ]; then
  head -1 "$SCRIPT_DIR/account.example.csv" > "$ACCOUNT_CSV"
  chown "$RUN_USER:$RUN_USER" "$ACCOUNT_CSV"
  chmod 0600 "$ACCOUNT_CSV"
  echo "    已创建 $ACCOUNT_CSV（仅表头）——请编辑填入真实账户凭证后重载"
  ACCOUNT_CSV_FRESH=1
else
  echo "    $ACCOUNT_CSV 已存在，跳过"
  ACCOUNT_CSV_FRESH=0
fi

# ——— 6. 生成 systemd 单元（User= 替换为真实用户；旧 webhook 自动保留） ———
echo "==> 生成 systemd 单元（User=$RUN_USER）"
SED_SCRIPT="$(mktemp)"
trap 'rm -f "$SED_SCRIPT"' EXIT
{
  echo "s|^User=.*|User=$RUN_USER|"
  echo "s|^WorkingDirectory=.*|WorkingDirectory=$DATA_DIR|"
  printf 's|^ExecStart=.*|ExecStart=%s \\\\|\n' "$BIN_DST"
  # ExecStart 的续行参数：路径类逐行替换（\\\\ 在 sed 输出中还原为续行符）。
  # 注意：--south-addr（HTTP 监听地址）不做替换，直接以 .service 中的值为准。
  printf 's|^  --data-dir .*|  --data-dir %s \\\\|\n' "$DATA_DIR"
  printf 's|^  --venue-config .*|  --venue-config %s/venue.toml \\\\|\n' "$CONF_DIR"
  printf 's|^  --account-file .*|  --account-file %s/account.csv \\\\|\n' "$DATA_DIR"
  printf 's|^  --south-secret-file .*|  --south-secret-file %s/south.key \\\\|\n' "$DATA_DIR"
  # 继承执行用户的代理环境（binance/deribit 等外网访问可能依赖代理；
  # systemd 单元默认不读用户 shell 的 .proxy_env）。探测顺序：
  # 1) sudo -E 透传的 http_proxy/https_proxy 环境变量；
  # 2) 执行用户的 ~/.proxy_env（sudo env_reset 会抹掉变量，故兜底读文件）。
  # https 目标（binance/deribit 全是 https/wss）只认 https_proxy——http_proxy
  # 对 TLS 请求无效，因此两行都要注入（空格分隔，systemd 多变量合法语法）。
  # 直连外网的机器两处都没有 → Environment 保持空。
  collect_proxy() {
    # 输出形如 "http_proxy=... https_proxy=..."（存在才输出对应项）。
    local out="" v val
    for v in http_proxy https_proxy; do
      val="${!v:-}"
      [ -n "$val" ] && out="$out $v=$val"
    done
    # 去掉前导空格（bash 无 ${#out:+...} 嵌套展开，用 xargs 稳妥去首尾空白）。
    echo -n "$out" | xargs
  }
  PROXY_VAL="$(collect_proxy)"
  if [ -z "$PROXY_VAL" ] && [ "$RUN_USER" != "root" ]; then
    proxy_env_file="/home/$RUN_USER/.proxy_env"
    if [ -f "$proxy_env_file" ]; then
      # 取 export 的 https_proxy 与 http_proxy 两行（https 优先），去 export 前缀。
      local_vals=""
      for name in https_proxy http_proxy; do
        line=$(grep -hE "^export $name=" "$proxy_env_file" | head -1 || true)
        if [ -n "$line" ]; then
          local_vals="$local_vals ${line#export }"
        fi
      done
      PROXY_VAL="${local_vals# }"
    fi
  fi
  # 注意：systemd 的 Environment='a=1 b=2' 会把整串当成【一个】变量的值（空格
  # 不拆分），reqwest 读到 "http://x http://y" 即代理失效。因此每个代理变量
  # 单独一行 Environment=。模板只有一个 Environment= 占位行：第一个变量替换它，
  # 其余变量追加在其后（sed 追加用 a\ 太脆，直接把占位行一次替换为多行文本）。
  if [ -n "$PROXY_VAL" ]; then
    # 把 "a=x b=y" 拼成 sed 替换文本 "Environment=a=x\nEnvironment=b=y"。
    env_lines=""
    for pair in $PROXY_VAL; do
      env_lines="${env_lines:+$env_lines\\n}Environment=$pair"
    done
    printf 's|^Environment=.*|%s|\n' "$env_lines"
  fi
  echo "s|^StandardOutput=.*|StandardOutput=append:$LOG_DIR/${PROJECT_NAME}.log|"
  echo "s|^StandardError=.*|StandardError=append:$LOG_DIR/${PROJECT_NAME}.log|"
  # webhook 回填保护：用户可能在 .service 中直接填写了 webhook；
  # 仅当 .service 仍为占位符而旧安装单元（/etc）已填过时，才从旧值回填，
  # 避免重跑脚本时把用户在 .service 里新填的 webhook 覆盖回旧值。
  if [ -f "$SERVICE_SRC" ] && [ -f "$SERVICE_DST" ]; then
    for var in OPEN CLOSE TEST MANUAL_CLOSE POSITION; do
      old_url=$(sed -n "s/.*--notify-${var,,}-url '\([^']*\)'.*/\1/p" "$SERVICE_DST" | head -1)
      if [ -n "$old_url" ] && [[ "$old_url" != REPLACE_WITH_* ]]; then
        esc_url=$(printf '%s' "$old_url" | sed 's/[&|]/\\&/g')
        echo "s|'REPLACE_WITH_${var}_WEBHOOK'|'${esc_url}'|"
      fi
    done
  fi
} > "$SED_SCRIPT"
if [ "$(wc -l < "$SED_SCRIPT")" -gt 9 ]; then
  echo "    检测到旧单元中已配置的 webhook，自动保留"
fi
sed -f "$SED_SCRIPT" "$SERVICE_SRC" > "$SERVICE_DST"
chmod 0644 "$SERVICE_DST"

# ——— 7. 注册 systemd 单元（不 enable、不启动；配置完成后用 bin/start.sh） ———
systemctl daemon-reload

echo
echo "部署完成（服务尚未启动）。"
echo
echo "下一步（请按顺序完成）："
echo "  1) 添加账户：${OPT_ROOT}/bin/account-tool add-account"
echo "     （CTP 账户的 name 必须与 [ctp].md_account 完全一致）"
echo "  2) 如需使用 CTP，编辑 ${CONF_DIR}/venue.toml 的 [ctp] md_front / md_account"
echo "  3) 启动服务：sudo ${OPT_ROOT}/bin/start.sh"
echo "     之后账户变更用 ${PROJECT_NAME} reload（无需重启）；[ctp] 变更重跑 start.sh"
echo
echo "  安装目录：$OPT_ROOT（属主 $RUN_USER，日常操作无需 sudo）"
echo "  管理：${PROJECT_NAME} {status|start|stop|restart|log|reload|conf}"
echo "        （已软链到 $MANAGE_LINK；start/stop/restart 内部仍走 sudo）"
echo "  日志：tail -f $LOG_DIR/${PROJECT_NAME}.log"
echo "  CTP 运行库：$CTP_LIB_DIR（trader_so/md_so 已写入 conf/venue.toml）"
echo "  查看生效配置：cat $SERVICE_DST（自定义监听地址/webhook 等：编辑 deploy/trading-service.service 后重跑本脚本）"
echo
if [ "$ACCOUNT_CSV_FRESH" -eq 1 ]; then
  echo "提示：account-tool add-account 会逐项提问、secret 不回显、写入前自动加密；"
  echo "     也可手工编辑 $ACCOUNT_CSV（表头同 deploy/account.example.csv）。"
else
  echo "账户文件已存在；新增/修改账户后执行 ${PROJECT_NAME} reload 热加载。"
fi
echo
echo "可选：需要企业微信播报时，编辑 $SERVICE_DST 中 REPLACE_WITH_*_WEBHOOK 五项，"
echo "然后 systemctl daemon-reload && sudo ${OPT_ROOT}/bin/start.sh；"
echo "已填过的 webhook 在重跑本脚本时会被自动保留。"
echo "启动后 WS 接入密钥：$DATA_DIR/south.key（分发给策略方）。"
echo "卸载：sudo ./deploy/uninstall.sh"
