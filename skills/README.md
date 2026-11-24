# Skills

本仓库内置两个 agent skill，用于交易服务的人工验收测试与交易操作。

| Skill | 用途 |
|-------|------|
| `trade-acceptance` | 验收测试执行器：读取验收测试文档，逐项通过 `trade-ops` 执行，产出含证据的报告 |
| `trade-ops` | 交易助手：账户、订单、仓位、风控规则的查询与操作 |

## 目录结构

```
skills/
├── trade-acceptance/
│   ├── SKILL.md
│   └── reference/          # 执行规则、17 个基准测试项、报告模板、现价脚本
└── trade-ops/
    ├── SKILL.md
    └── reference/          # 接口参考、配置、命令记忆
```

## 在 Claude Code 中启用

Claude Code 自动发现 `.claude/skills/`。在仓库根目录建立相对软链即可：

```bash
mkdir -p .claude/skills
ln -sfn ../../skills/trade-acceptance .claude/skills/trade-acceptance
ln -sfn ../../skills/trade-ops       .claude/skills/trade-ops
```

## 使用

```bash
# 验收测试（示例）
/trade-acceptance 按 docs/acceptance/ctp验收测试输入.md 执行验收测试
```

`trade-ops/reference/config.yaml` 中的服务地址按本地环境调整；`last_commands.json` 为运行时命令记忆，可清空。

## 维护

内容随仓库版本管理，修改请提交 PR；接口或行为变更时同步更新 `trade-ops/reference/` 与 `trade-acceptance/reference/`。
