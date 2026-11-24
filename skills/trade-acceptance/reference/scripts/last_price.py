#!/usr/bin/env python3
"""只读取现价：通过南向 WS 的 get_klines 取最新 1m 收盘价。

用途：验收测试在执行前获取当前价格，用于调整限价单价格、价格条件与风控阈值。
本脚本只做行情查询，不发送任何交易写操作。

用法：
  python3 last_price.py --exchange binance --symbol BTCUSDT
  python3 last_price.py --exchange ctp --symbol rb2701 --base http://127.0.0.1:9100
  python3 last_price.py --exchange deribit --symbol BTC-27DEC26-100000-C --insecure

服务地址：
  优先级 --base > TRADE_API_BASE > ~/.config/trade-acceptance/config.yaml 的 api.base
  > trade-ops reference/config.yaml 的 api.base；
  同一台机器默认 http://127.0.0.1:9100，远程时传 ws/http 地址即可，无需改代码。

握手：
  默认读取服务端共享密钥做 HMAC-SHA256 签名；路径优先级：
  --key-file > SOUTH_KEY_FILE > TRADE_SERVICE_DATA_DIR/south.key
  > ~/.config/trade-acceptance/config.yaml 的 south.key_file > ./data/south.key；
  服务端以 --south-dev-insecure 启动时可用 --insecure 跳过签名。

输出（stdout，JSON）：
  {"exchange": "...", "symbol": "...", "close": 123.45, "end_time": 1763424000000, "source": "get_klines"}
失败时输出 {"error": "..."} 到 stderr，退出码非 0。密钥永不出现在任何输出中。
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import re
import secrets
import sys
from pathlib import Path
from urllib.parse import urlencode, urlsplit, urlunsplit

try:
    from websockets.sync.client import connect
except ImportError:  # 兼容旧版本 websockets
    connect = None


def resolve_key_file(cli_value: str | None) -> str | None:
    """密钥路径优先级：--key-file > SOUTH_KEY_FILE > TRADE_SERVICE_DATA_DIR/south.key
    > 用户配置 south.key_file > ./data/south.key。"""
    if cli_value:
        return cli_value
    env = os.environ.get("SOUTH_KEY_FILE", "").strip()
    if env:
        return env
    data_dir = os.environ.get("TRADE_SERVICE_DATA_DIR", "").strip()
    if data_dir:
        return str(Path(data_dir).expanduser() / "south.key")
    user = _read_user_config().get("key_file")
    if user:
        return user
    return "./data/south.key"


def _trade_ops_config_path() -> Path:
    """trade-ops 技能配置路径（与交易助手共用一份服务地址）。"""
    env = os.environ.get("TRADE_OPS_CONFIG", "").strip()
    if env:
        return Path(env).expanduser()
    return Path.home() / ".claude" / "skills" / "trade-ops" / "reference" / "config.yaml"


def _user_config_path() -> Path:
    """用户级配置（仓库外，git pull 不会冲突）：$XDG_CONFIG_HOME/trade-acceptance/config.yaml。"""
    root = os.environ.get("XDG_CONFIG_HOME", "").strip()
    base = Path(root).expanduser() if root else Path.home() / ".config"
    return base / "trade-acceptance" / "config.yaml"


def _read_user_config() -> dict:
    """读取用户级 config.yaml 的 api.base / south.key_file（无 YAML 依赖的最小解析）。"""
    try:
        text = _user_config_path().read_text(encoding="utf-8")
    except OSError:
        return {}
    out = {}
    m = re.search(r"^\s*base:\s*[\"']?(https?://[^\"'\s#]+)", text, re.MULTILINE)
    if m:
        out["base"] = m.group(1)
    m = re.search(r"^\s*key_file:\s*[\"']?([^\"'\s#]+)", text, re.MULTILINE)
    if m:
        out["key_file"] = m.group(1)
    return out


def default_base() -> str:
    """服务地址来源优先级：TRADE_API_BASE > trade-ops config.yaml api.base > 本机默认。"""
    env = os.environ.get("TRADE_API_BASE", "").strip()
    if env:
        return env
    user = _read_user_config().get("base")
    if user:
        return user
    cfg = _trade_ops_config_path()
    try:
        text = cfg.read_text(encoding="utf-8")
    except OSError:
        return "http://127.0.0.1:9100"
    # 只取 api.base，避免引入 YAML 依赖；支持引号与行尾注释。
    m = re.search(r"^\s*base:\s*[\"']?(https?://[^\"'\s#]+)", text, re.MULTILINE)
    return m.group(1) if m else "http://127.0.0.1:9100"


def ws_url(base: str, strategy: str, key_file: str | None, insecure: bool) -> str:
    """把 REST base 转成南向 WS URL，并按需追加签名参数。"""
    parts = urlsplit(base)
    scheme = "wss" if parts.scheme == "https" else "ws"
    path = (parts.path.rstrip("/") + "/ws") if parts.path else "/ws"
    query = {"strategy_name": strategy}

    if not insecure:
        if not key_file:
            raise ValueError("缺少 --key-file（或使用 --insecure 连接开发态服务）")
        try:
            with open(key_file, "r", encoding="utf-8") as f:
                secret = f.read().strip()
        except OSError as exc:
            raise ValueError(
                f"读取密钥文件失败: {key_file} ({exc})；"
                "请指定 --key-file，或对 --south-dev-insecure 服务使用 --insecure"
            ) from exc
        if not secret:
            raise ValueError(f"密钥文件为空: {key_file}")
        nonce = secrets.token_hex(16)
        signature = hmac.new(
            secret.encode("utf-8"),
            (strategy + nonce).encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        query["nonce"] = nonce
        query["signature"] = signature

    return urlunsplit((scheme, parts.netloc, path, urlencode(query), ""))


def fetch_last_close(url: str, exchange: str, symbol: str, timeout: float) -> dict:
    """连接南向 WS，发 get_klines，返回最新一根 bar 的收盘价。"""
    request = {
        "option": "get_klines",
        "exchange": exchange,
        "symbol": symbol,
        "limit": 1,
    }
    with connect(url, open_timeout=timeout, close_timeout=timeout) as ws:
        ws.send(json.dumps(request, ensure_ascii=False))
        while True:
            raw = ws.recv(timeout=timeout)
            try:
                frame = json.loads(raw)
            except json.JSONDecodeError:
                continue
            kind = frame.get("type")
            if kind == "error":
                raise ValueError(frame.get("message") or frame.get("error") or "服务端返回 error 帧")
            if kind != "kline_history":
                continue
            klines = frame.get("klines") or []
            if not klines:
                raise ValueError(
                    f"{exchange}/{symbol} 无本地 K线数据（可能休市或品种未订阅，"
                    "可回退到仓位标记价）"
                )
            bar = klines[-1]
            return {
                "exchange": exchange,
                "symbol": symbol,
                "close": bar.get("close"),
                "end_time": bar.get("end_time"),
                "source": "get_klines",
            }


def main() -> int:
    parser = argparse.ArgumentParser(description="只读取最新 1m 收盘价（南向 WS get_klines）")
    parser.add_argument("--exchange", required=True, help="binance / deribit / ctp / hyperliquid")
    parser.add_argument("--symbol", required=True, help="交易所原生标的，如 BTCUSDT、rb2701")
    parser.add_argument(
        "--base",
        default=None,
        help="服务地址；缺省取 TRADE_API_BASE 或 trade-ops config.yaml 的 api.base",
    )
    parser.add_argument("--strategy", default="acceptance-probe", help="握手策略名")
    parser.add_argument(
        "--key-file",
        default=None,
        help="南向共享密钥文件（优先级：--key-file > SOUTH_KEY_FILE > TRADE_SERVICE_DATA_DIR/south.key > ./data/south.key）",
    )
    parser.add_argument("--insecure", action="store_true", help="跳过签名（仅限 --south-dev-insecure 服务）")
    parser.add_argument("--timeout", type=float, default=10.0, help="连接/接收超时秒数")
    args = parser.parse_args()

    if connect is None:
        print(json.dumps({"error": "缺少依赖 websockets，请先 pip install websockets"}, ensure_ascii=False))
        return 2

    try:
        base = args.base or default_base()
        url = ws_url(base, args.strategy, resolve_key_file(args.key_file), args.insecure)
        result = fetch_last_close(url, args.exchange, args.symbol, args.timeout)
    except Exception as exc:  # 统一转成可读错误，避免堆栈干扰验收流程
        print(json.dumps({"error": str(exc)}, ensure_ascii=False))
        return 2

    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
