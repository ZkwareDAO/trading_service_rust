# 安全策略

## 支持版本

| 版本 | 支持状态 |
|---|---|
| main 分支最新 | ✅ 支持 |
| 其它历史提交 | ❌ 不支持 |

## 报告漏洞

**请勿通过公开 issue 报告安全漏洞**（本项目涉及交易所凭证与真实资金）。

请通过 GitHub 的 **Private vulnerability reporting**（仓库 Settings → Security →
Report a vulnerability）私下报告。我们会在 7 天内确认并跟进修复。

报告时请包含：影响范围、复现方式、可能的缓解措施。请勿在报告中附带真实凭证。

## 安全设计要点（供审计参考）

- **凭证存储**：`account.csv` 的 api_secret 支持 AES-256-GCM 加密形态
  （`enc.v1.*`，主密钥 `account.key` 为 32 字节随机数，0600）；
- **安全投影**：账户 REST 响应绝不返回 api_key / api_secret / api_passphrase；
- **南向鉴权**：WS 握手强制 HMAC-SHA256 签名 + nonce 防重放（5 分钟 TTL、恒定时间比较）；
- **最小权限**：服务以普通用户运行（systemd `User=`），绑定 127.0.0.1 回环，
  对外建议经 TLS 反代（wss）；
- **密钥文件不入库**：`account.csv`、`*.key`、`data/` 均在 `.gitignore`。

## 已知边界

- 管理接口 `POST /admin/accounts/reload` 无独立鉴权，依赖“仅绑定回环 + 不对外暴露”
  的部署约束（见 [docs/部署文档.md](docs/部署文档.md)）；
- 测试网/正式网按账户属性（`is_testnet`）选择，请勿在共享环境开启明文 secret。
