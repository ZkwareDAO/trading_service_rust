#!/usr/bin/env python3
"""venue.toml 部署合并：只补 live 缺失的键，不覆盖已有值、不删键、不改注释。

用法：
  python3 merge_venue_config.py --template config/venue.toml \
      --live /opt/trading_service_rust/conf/venue.toml [--dry-run]

规则（方案 B）：
- 模板有、live 缺的标量/数组：插入到对应 `[section]` 头之后；
  live 没有该 section 时，追加 `[section]` 到文件尾；
- 模板有、live 缺的子表：以 `[a.b]` 段追加到文件尾；
- live 已有的键一律不动；live 多出的键保留（代码对未知键宽容）；
- 只在文本层面插入，不重写已有行，因此注释/格式保持不变；
- 模板/live 类型冲突（一边是子表、一边是标量）只提示、不改；
- live 为符号链接时写其真实目标，链接本身保留；
- 写入前自动备份 live 为 `<live>.bak.<YYYYmmdd-HHMMSS>`，并保留原文件权限/属主；
- 幂等：没有缺失键时不写文件、退出 0。
"""

import argparse
import datetime
import json
import os
import shutil
import sys
import time
import tomllib


def load_toml(path: str) -> dict:
    with open(path, "rb") as f:
        return tomllib.load(f)


def fmt_value(v) -> str:
    """把 Python 值渲染为 TOML 字面量（覆盖配置里用到的 str/int/float/bool/数组）。"""
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (int, float)):
        return repr(v)
    if isinstance(v, str):
        # JSON 字符串转义与 TOML basic string 兼容（\\n/\\t/\\uXXXX 均合法）。
        return json.dumps(v, ensure_ascii=False)
    if isinstance(v, list):
        return "[" + ", ".join(fmt_value(x) for x in v) + "]"
    if isinstance(v, (datetime.datetime, datetime.date, datetime.time)):
        return v.isoformat()
    raise TypeError(f"不支持的配置值类型：{type(v).__name__}")


def collect_missing(template, live, prefix=(), conflicts=None):
    """返回模板中有、live 中缺的 (path, value)；整棵缺失子表按子树返回。

    模板是子表而 live 是标量（或反之）属于类型冲突：不自动处理，记入 conflicts
    由调用方提示人工处理（绝不覆盖已有值）。
    """
    conflicts = conflicts if conflicts is not None else []
    out = []
    for key, value in template.items():
        path = prefix + (key,)
        if key not in live:
            out.append((path, value))
        elif isinstance(value, dict) and isinstance(live.get(key), dict):
            out.extend(collect_missing(value, live[key], path, conflicts))
        elif isinstance(value, dict) or isinstance(live.get(key), dict):
            conflicts.append(".".join(path))
    return out


def flatten_missing(path, value):
    """把缺失项展开为 (section_path, key, toml_literal)。"""
    if isinstance(value, dict):
        items = []
        for k, v in value.items():
            items.extend(flatten_missing(path + (k,), v))
        return items
    return [(path[:-1], path[-1], fmt_value(value))]


def section_header(sec) -> str:
    return "[" + ".".join(sec) + "]"


def main() -> int:
    ap = argparse.ArgumentParser(description="只补 venue.toml 缺失键（方案 B）")
    ap.add_argument("--template", required=True, help="仓库模板配置路径")
    ap.add_argument("--live", required=True, help="机器上正在使用的配置路径")
    ap.add_argument("--dry-run", action="store_true", help="只打印将要补的键，不写文件")
    args = ap.parse_args()

    try:
        template = load_toml(args.template)
        live = load_toml(args.live)
    except (OSError, tomllib.TOMLDecodeError) as e:
        print(f"读取/解析配置失败：{e}", file=sys.stderr)
        return 2

    conflicts = []
    missing = collect_missing(template, live, conflicts=conflicts)
    for c in conflicts:
        print(f"类型冲突（跳过，请人工处理）：{c}", file=sys.stderr)
    if not missing:
        print("venue.toml 无缺失键（模板与 live 兼容）")
        return 0

    # 分组：顶层键 / 已有 section 的缺失键 / 需追加的新 section。
    top_keys = []
    by_section = {}
    for path, value in missing:
        for sec, key, literal in flatten_missing(path, value):
            if sec:
                by_section.setdefault(sec, []).append((key, literal))
            else:
                top_keys.append((key, literal))

    # live 若为符号链接：写其真实目标，链接本身保留。
    live_path = os.path.realpath(args.live) if os.path.islink(args.live) else args.live
    with open(live_path, "r", encoding="utf-8") as f:
        text = f.read()
    lines = text.splitlines(keepends=True)
    if lines and not lines[-1].endswith("\n"):
        lines[-1] += "\n"

    # 已存在的 section 头行号（忽略 [[array-of-tables]]）。
    header_idx = {}
    for i, line in enumerate(lines):
        s = line.strip()
        if s.startswith("[") and not s.startswith("[[") and s.endswith("]"):
            header_idx[s] = i

    inserts = {}  # 行号 -> 待插入行（插在该行号之前）
    appended = []  # 文件尾追加的文本
    added = []

    # 顶层键：插在第一个 section 头之前（无 section 则追加到文件尾）。
    if top_keys:
        chunk = [f"{k} = {v}\n" for k, v in top_keys]
        if header_idx:
            inserts.setdefault(min(header_idx.values()), []).extend(chunk)
            added.extend(f"(顶层) {k}" for k, _ in top_keys)
        else:
            appended.extend(chunk)
            added.extend(f"(顶层) {k}" for k, _ in top_keys)

    # 已有 section 的缺失键：紧跟 section 头插入；section 不存在则整段追加到文件尾。
    for sec, items in by_section.items():
        hdr = section_header(sec)
        chunk = [f"{k} = {v}\n" for k, v in items]
        if hdr in header_idx:
            inserts.setdefault(header_idx[hdr] + 1, []).extend(chunk)
        else:
            appended.extend([f"\n{hdr}\n"] + chunk)
        added.extend(f"{hdr} {k}" for k, _ in items)

    if appended:
        lines.extend(appended)

    for idx in sorted(inserts, reverse=True):
        lines[idx:idx] = inserts[idx]

    new_text = "".join(lines)
    if new_text == text:
        print("venue.toml 无缺失键（模板与 live 兼容）")
        return 0

    print("将补充缺失键：" + ", ".join(sorted(added)))
    if args.dry_run:
        print("（dry-run：未写入）")
        return 0

    try:
        st = os.stat(live_path)
        stamp = time.strftime("%Y%m%d-%H%M%S")
        backup = f"{live_path}.bak.{stamp}"
        shutil.copy2(live_path, backup)
        tmp = f"{live_path}.tmp.{os.getpid()}"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(new_text)
            # 保留原文件权限与属主：部署可能以 root 运行，避免 live 变成 root:root/0644。
            shutil.copystat(live_path, tmp)
            try:
                os.chown(tmp, st.st_uid, st.st_gid)
            except OSError:
                pass
            os.replace(tmp, live_path)
        finally:
            if os.path.exists(tmp):
                os.unlink(tmp)
    except OSError as e:
        print(f"写入配置失败：{e}", file=sys.stderr)
        return 2
    print(f"已写入 {live_path}（备份：{backup}）")
    return 0


if __name__ == "__main__":
    sys.exit(main())
