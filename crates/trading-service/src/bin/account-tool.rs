//! 账户凭证加密 / 账户管理小工具（生产前置）。
//!
//! 复用与服务完全一致的 AES-256-GCM 原语（`trading_models::SecretCipher`）：
//! * `gen-key`：生成 32 字节主密钥，0600 落盘（即 account.key）
//! * `encrypt`：加密单个明文，输出 `enc.v1.*`
//! * `migrate`：把**旧宽表** account.csv 逐行迁移为新 7 列格式（原文件备份
//!   `.bak`；`--key` 提供时 credentials 整串加密回写）
//! * `add-account`：校验并追加一条账户到 account.csv（文件不存在先建表头；
//!   同名账户默认拒绝，`--update` 时按 name 整行替换）。新 7 列格式：公共列 +
//!   `credentials` JSON（按 exchange 选 schema，binance/ctp 模板见
//!   [`credentials_template`]），`--key` 提供时写入前 `encrypt_if_plaintext`
//!   整串加密
//!
//! 已是 `enc.v1.*` / 空值会原样跳过（encrypt/encrypt-csv）。
//!
//! # 交互式与脚本两种用法
//!
//! 所有参数都是**可选**的：交互终端下缺什么就问什么（credentials JSON 输入
//! 不回显）；非交互管道/脚本场景则按命令行参数执行，必填参数缺失时报错退出，
//! 方便 CI / 部署脚本无人工调用。
//!
//! 注意：account.key 存的是 32 字节原始密钥（非 hex），与服务读取口径一致。
//! 主密钥丢失后历史密文将无法解密，请妥善备份。
//!
//! CSV 口径（与 trading-store 的读取一致）：服务按 `name` 作为账户身份（id 由
//! 服务端按 name 稳定哈希生成，CSV 中的 id/created_at 列不参与加载），因此本
//! 工具写入 id=0、created_at=0 即可；`updated_at` 由调用方按需携带（缺省 0）。

use std::io::{IsTerminal, Read, Write};
use std::path::{Path, PathBuf};

use anyhow::Context as _;
use clap::{Parser, Subcommand};
use trading_exchange::port::credential_from_json;
use trading_models::account::SecretCipher;

/// account.csv 的标准表头（新 7 列：6 公共列 + credentials，与
/// trading_models::ACCOUNT_CSV_HEADER 一致，测试钉死等价）。
pub const ACCOUNT_CSV_HEADER: [&str; 7] = [
    "id",
    "name",
    "exchange",
    "credentials",
    "deleted",
    "created_at",
    "updated_at",
];

/// 账户重载接口（服务管理 API；本机回环，与 service 默认 --south-addr 同口）。
const RELOAD_URL: &str = "http://127.0.0.1:9100/admin/accounts/reload";

/// 部署机的标准数据目录（deploy.sh 的固定安装布局）。
const OPT_DATA_DIR: &str = "/opt/trading_service_rust/data";

/// 数据目录默认值解析（优先级从高到低）：
/// 1. 环境变量 `TRADING_DATA_DIR`（manage.sh 会导出，可显式覆盖）；
/// 2. `/opt/trading_service_rust/data`（部署布局存在才用——裸跑工具也能命中）；
/// 3. `./data`（本地开发回退）。
fn default_data_dir() -> String {
    if let Ok(v) = std::env::var("TRADING_DATA_DIR") {
        if !v.trim().is_empty() {
            return v;
        }
    }
    if std::path::Path::new(OPT_DATA_DIR).is_dir() {
        return OPT_DATA_DIR.to_string();
    }
    "./data".to_string()
}

/// 基于 [`default_data_dir`] 组装默认文件路径。
fn default_data_file(name: &str) -> String {
    format!("{}/{name}", default_data_dir().trim_end_matches('/'))
}

/// 账户凭证加密 / 账户管理工具。
#[derive(Parser)]
#[command(name = "account-tool")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// 生成 32 字节随机主密钥，以 0600 原子写入指定路径。
    GenKey {
        /// 主密钥输出路径（如 ./data/account.key）。
        #[arg(long)]
        key: Option<PathBuf>,
    },

    /// 加密单个明文 secret，打印 enc.v1.* 密文。
    Encrypt {
        /// 主密钥文件（32 字节 raw）。
        #[arg(long)]
        key: Option<PathBuf>,
        /// 明文 secret；不传则交互提问或从 stdin 读取（去除首尾空白）。
        #[arg(long)]
        secret: Option<String>,
    },

    /// 把**旧宽表** account.csv 迁移为新 7 列格式（原文件备份 .bak）。
    ///
    /// 逐行按 exchange 拼 credentials JSON（CTP 行 api_key=user_id、
    /// api_passphrase=password；旧 api_secret 列若已是 enc.v1. 密文需 `--key`
    /// 解密还原），`--key` 提供时整串 `encrypt_if_plaintext` 加密回写。
    /// 已是新格式（无 api_key 列）则原样跳过。
    Migrate {
        /// 主密钥文件（32 字节 raw）；旧 secret 列有密文时必须提供。
        #[arg(long)]
        key: Option<PathBuf>,
        /// account.csv 路径。
        #[arg(long)]
        csv: Option<PathBuf>,
    },

    /// 追加一条账户到 account.csv（新 7 列格式；文件不存在则先创建表头）。
    ///
    /// credentials 为**整串 JSON**：REST 签名所（binance/deribit/hyperliquid）
    /// 用 ApiSecret schema，CTP 期货用 Ctp schema（交互终端下按 exchange 打印
    /// 对应模板）。写入前按 exchange 校验 JSON 合法性；`--key` 提供时
    /// `encrypt_if_plaintext` 整串加密（生产推荐），缺省明文写入（服务会告警）。
    AddAccount {
        /// account.csv 路径（数据目录下，如 ./data/account.csv）。
        #[arg(long)]
        csv: Option<PathBuf>,
        /// 账户名（同一 exchange 内唯一；服务据此生成稳定 id）。
        #[arg(long)]
        name: Option<String>,
        /// 交易所精确名（binance / deribit / hyperliquid / ctp …）。
        #[arg(long)]
        exchange: Option<String>,
        /// credentials JSON 明文（整串）；不传则交互按模板提问（不回显）或读 stdin。
        #[arg(long)]
        credentials: Option<String>,
        /// 主密钥文件；提供时写入前先加密为 enc.v1.*（生产推荐）。
        /// 缺省则按明文写入（服务启动会告警，仅限本地联调）。
        #[arg(long)]
        key: Option<PathBuf>,
        /// 同名账户已存在时，整行替换而不是报错退出。
        #[arg(long, default_value_t = false)]
        update: bool,
    },
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::GenKey { key } => {
            let key = match key {
                Some(k) => k,
                None => prompt_path("主密钥输出路径", &default_data_file("account.key"))?,
            };
            let raw = SecretCipher::random_key();
            if let Some(parent) = key.parent() {
                if !parent.as_os_str().is_empty() {
                    std::fs::create_dir_all(parent)?;
                }
            }
            // 原子 0600 新建，与服务首启生成 account.key 完全一致。
            store_core::fs::write_new_0600(&key, &raw)?;
            println!("已生成主密钥（0600）：{}", key.display());
            println!("请妥善备份；丢失后已加密的 credentials 无法解密。");
        }
        Cmd::Encrypt { key, secret } => {
            let key = match key {
                Some(k) => k,
                None => prompt_path("主密钥文件路径", &default_data_file("account.key"))?,
            };
            let cipher = load_cipher(&key)?;
            let plain = read_secret_input(secret, "API secret / 私钥（输入不回显）：")?;
            println!("{}", cipher.encrypt(&plain)?);
        }

        Cmd::Migrate { key, csv } => {
            let csv = match csv {
                Some(c) => c,
                None => prompt_path("account.csv 路径", &default_data_file("account.csv"))?,
            };
            let cipher = match &key {
                Some(k) => Some(load_cipher(k)?),
                None => None,
            };
            if cipher.is_none() {
                eprintln!("警告：未提供主密钥，迁移产出的 credentials 将以明文 JSON 写入（仅限本地联调）。");
            }
            let report = migrate_csv(&csv, cipher.as_ref())?;
            if report.migrated == 0 {
                println!(
                    "未检测到旧宽表格式（表头无 api_key 列），无需迁移：{}",
                    csv.display()
                );
            } else {
                println!(
                    "已迁移 {} 条账户到新 7 列格式：{}",
                    report.migrated,
                    csv.display()
                );
                if let Some(b) = report.backup {
                    println!("原文件已备份：{}（验证无误后可删除）", b.display());
                }
                println!("手工触发重载：curl -X POST {RELOAD_URL}");
            }
        }

        Cmd::AddAccount {
            csv,
            name,
            exchange,
            credentials,
            key,
            update,
        } => {
            // —— 逐项取值：参数优先，缺什么在交互终端下问什么 ——
            let csv = match csv {
                Some(c) => c,
                None => prompt_path("account.csv 路径", &default_data_file("account.csv"))?,
            };
            let name = match name {
                Some(n) => n,
                None => prompt_required("账户名（同一交易所内唯一）")?,
            };
            let exchange = match exchange {
                Some(e) => e,
                None => prompt_required("交易所（binance / deribit / hyperliquid / ctp …）")?,
            };
            // 按 exchange 打印 credentials JSON 模板，再整段读取。
            if interactive() {
                println!(
                    "credentials JSON 模板（{}）：\n  {}",
                    exchange,
                    credentials_template(&exchange)
                );
            }
            let plain_json =
                read_secret_input(credentials, "credentials JSON（整串输入，不回显）：")?;
            if plain_json.is_empty() {
                anyhow::bail!(
                    "credentials 为空：请用 --credentials 传入、交互输入或从 stdin 提供整串 JSON"
                );
            }
            // 写入前按 exchange 校验 JSON（schema 反序列化，缺必填字段即报错）。
            credential_from_json(&exchange, &plain_json).map_err(|e| {
                anyhow::anyhow!("credentials JSON 与交易所 `{exchange}` 不匹配：{e:#}")
            })?;
            let key = match key {
                Some(k) => Some(k),
                None if interactive() => {
                    prompt_path_opt("主密钥路径（回车跳过 = 明文写入，仅限联调）")?
                }
                None => None,
            };

            // 主密钥提供 → 写入前整串加密（生产路径）；缺省按明文（联调，服务会告警）。
            let stored_credentials = match &key {
                Some(k) => load_cipher(k)?.encrypt_if_plaintext(&plain_json)?.0,
                None => {
                    eprintln!(
                        "警告：未提供主密钥，credentials 将以明文写入（仅限本地联调，服务启动会告警）。"
                    );
                    plain_json
                }
            };

            // 组装行（列序与表头一致）；id/created_at 写 0——服务按 name 生成稳定 id。
            let record = build_account_row(
                &ACCOUNT_CSV_HEADER,
                &AccountRowInput {
                    name: &name,
                    exchange: &exchange,
                    credentials: &stored_credentials,
                    updated_at: 0,
                },
            )
            .map_err(|e| anyhow::anyhow!("{e}"))?;

            // 读现有文件：不存在则先建表头；同名行按 update 决定替换或拒绝
            // （交互终端下现场询问是否覆盖）。
            let (headers, mut rows) = read_csv_or_header(&csv, &ACCOUNT_CSV_HEADER)?;
            let update = resolve_update(&headers, &rows, &name, update)?;
            let replaced = upsert_row(&headers, &mut rows, &record, "name", update)?;
            let mut writer = csv::Writer::from_writer(Vec::new());
            writer.write_record(headers.iter())?;
            for row in &rows {
                writer.write_record(row.iter())?;
            }
            let bytes = writer.into_inner()?;

            // 原子回写（tmp + rename）；新建文件按 0600（内容含私钥，从严）。
            write_csv_atomic(&csv, &bytes)?;
            if replaced {
                println!("已更新账户 `{name}`：{}", csv.display());
            } else {
                println!("已追加账户 `{name}`（{exchange}）：{}", csv.display());
            }

            // 交互模式：写完即问是否触发 reload（默认是，一条命令走完添加+生效）；
            // 非交互：不主动调用（脚本/CI 自己控制时机），只给手工命令。
            if interactive() && prompt_yes_no("立即触发账户重载？", true)? {
                // CLI 的 main 非 async：用短生命周期 runtime 阻塞跑这一次 HTTP。
                let reload = || async { trigger_reload(RELOAD_URL).await };
                match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(anyhow::Error::from)
                    .and_then(|rt| rt.block_on(reload()))
                {
                    Ok(()) => println!("已通知服务重载账户。"),
                    Err(e) => eprintln!("重载失败（账户已写入文件，稍后可手工重试）：{e}"),
                }
            } else {
                println!("手工触发重载：curl -X POST {RELOAD_URL}");
            }
        }
    }
    Ok(())
}

/// 同名冲突处理：非交互 + 未开 --update → 报错；交互 → 现场询问是否覆盖。
fn resolve_update(
    headers: &csv::StringRecord,
    rows: &[Vec<String>],
    name: &str,
    update_flag: bool,
) -> anyhow::Result<bool> {
    let key_idx = headers
        .iter()
        .position(|h| h.trim() == "name")
        .ok_or_else(|| anyhow::anyhow!("CSV 缺少 name 列"))?;
    let exists = rows
        .iter()
        .any(|r| r.get(key_idx).map(String::as_str) == Some(name));
    if !exists || update_flag {
        return Ok(update_flag);
    }
    if !interactive() {
        anyhow::bail!("账户 `{name}` 已存在；如需覆盖请加 --update（将整行替换）");
    }
    prompt_yes_no(&format!("账户 `{name}` 已存在，覆盖原行？"), false)
}

// ———————— 交互辅助 ————————

/// 当前是否交互终端（决定"提问"还是"按参数执行"）。
fn interactive() -> bool {
    std::io::stdin().is_terminal()
}

/// 提问并读一行必填值；非交互终端直接报错（脚本场景参数必须给全）。
fn prompt_required(label: &str) -> anyhow::Result<String> {
    if !interactive() {
        anyhow::bail!("缺少必填参数：{label}（非交互环境请用命令行参数提供）");
    }
    loop {
        print!("{label}：");
        std::io::stdout().flush()?;
        let mut buf = String::new();
        std::io::stdin().read_line(&mut buf)?;
        let v = buf.trim().to_string();
        if !v.is_empty() {
            return Ok(v);
        }
        println!("  不能为空，请重新输入。");
    }
}

/// 提问读一个路径（带默认值）。
fn prompt_path(label: &str, default: &str) -> anyhow::Result<PathBuf> {
    Ok(PathBuf::from(prompt_default(label, default)?))
}

/// 提问读一个可选路径；空输入返回 None。
fn prompt_path_opt(label: &str) -> anyhow::Result<Option<PathBuf>> {
    Ok(prompt_optional(label)?.map(PathBuf::from))
}

/// 提问读一行可选值；空输入返回 None。
fn prompt_optional(label: &str) -> anyhow::Result<Option<String>> {
    if !interactive() {
        return Ok(None);
    }
    print!("{label}：");
    std::io::stdout().flush()?;
    let mut buf = String::new();
    std::io::stdin().read_line(&mut buf)?;
    let v = buf.trim().to_string();
    Ok((!v.is_empty()).then_some(v))
}

/// 提问读一行带默认值；空输入取默认。
fn prompt_default(label: &str, default: &str) -> anyhow::Result<String> {
    if !interactive() {
        return Ok(default.to_string());
    }
    print!("{label} [{default}]：");
    std::io::stdout().flush()?;
    let mut buf = String::new();
    std::io::stdin().read_line(&mut buf)?;
    let v = buf.trim().to_string();
    Ok(if v.is_empty() { default.to_string() } else { v })
}

/// y/N 提问；非交互按默认。
fn prompt_yes_no(label: &str, default: bool) -> anyhow::Result<bool> {
    if !interactive() {
        return Ok(default);
    }
    let hint = if default { "[Y/n]" } else { "[y/N]" };
    print!("{label} {hint}：");
    std::io::stdout().flush()?;
    let mut buf = String::new();
    std::io::stdin().read_line(&mut buf)?;
    match buf.trim().to_ascii_lowercase().as_str() {
        "y" | "yes" => Ok(true),
        "n" | "no" => Ok(false),
        "" => Ok(default),
        other => {
            println!("  无法识别 `{other}`，按默认处理。");
            Ok(default)
        }
    }
}

/// 隐感输入兜底（secret / credentials JSON）：显式入参 → trim 返回；
/// 交互 → 不回显提问（防密钥留在终端回滚缓冲/屏幕记录）；管道 → 读 stdin（trim）。
fn read_secret_input(provided: Option<String>, prompt: &str) -> anyhow::Result<String> {
    if let Some(s) = provided {
        return Ok(s.trim().to_string());
    }
    if interactive() {
        Ok(rpassword::prompt_password(prompt)?.trim().to_string())
    } else {
        let mut buf = String::new();
        std::io::stdin().read_to_string(&mut buf)?;
        Ok(buf.trim().to_string())
    }
}

/// 各所 credentials JSON 模板（交互提示用）：CTP 看穿式 7 件套；
/// 其余所（binance/deribit/hyperliquid 同构）共用 ApiSecret schema。
fn credentials_template(exchange: &str) -> &'static str {
    if exchange.trim().eq_ignore_ascii_case("ctp") {
        r#"{"broker_id":"9999","user_id":"100001","password":"pwd","app_id":"simnow_client","auth_code":"0000000000000000","investor_id":"","front_address":"tcp://182.254.243.31:40001","is_production_mode":false}"#
    } else {
        r#"{"api_key":"abc123","api_secret":"s3cret","api_passphrase":null,"position_mode":"one_way","is_testnet":false}"#
    }
}

/// 调用服务的账户重载接口；非 2xx 或连不上都返回错误（调用方仅告警，不影响已写文件）。
async fn trigger_reload(url: &str) -> anyhow::Result<()> {
    let resp = reqwest::Client::new()
        .post(url)
        .timeout(std::time::Duration::from_secs(5))
        .send()
        .await?;
    let status = resp.status();
    if status.is_success() {
        Ok(())
    } else {
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!(
            "HTTP {status}：{}",
            body.chars().take(200).collect::<String>()
        )
    }
}

// ———————— 纯函数（单元测试覆盖） ————————

/// migrate 的结果：迁移行数与原文件备份路径（no-op 时均空）。
struct MigrateReport {
    migrated: usize,
    backup: Option<PathBuf>,
}

/// 旧宽表 account.csv → 新 7 列格式：逐行按 exchange 拼 credentials JSON，
/// `--key` 提供时整串 `encrypt_if_plaintext` 加密；原文件备份 `.bak` 后
/// 原子回写。已是新格式（表头无 api_key 列）→ no-op。文件不存在报错。
fn migrate_csv(path: &Path, cipher: Option<&SecretCipher>) -> anyhow::Result<MigrateReport> {
    if !path.exists() {
        anyhow::bail!("account.csv 不存在：{}", path.display());
    }
    let (headers, rows) = read_csv_or_header(path, &ACCOUNT_CSV_HEADER)?;
    let name_idx = headers
        .iter()
        .position(|h| h.trim() == "name")
        .ok_or_else(|| anyhow::anyhow!("CSV 缺少 name 列"))?;
    if !headers.iter().any(|h| h.trim() == "api_key") {
        // 新格式（或空表）：无需迁移，也不做任何写动作。
        return Ok(MigrateReport {
            migrated: 0,
            backup: None,
        });
    }

    let mut new_rows: Vec<Vec<String>> = Vec::with_capacity(rows.len());
    for row in &rows {
        let name = row
            .get(name_idx)
            .cloned()
            .unwrap_or_default()
            .trim()
            .to_string();
        if name.is_empty() {
            tracing::warn!("跳过缺少 name 的账户行");
            continue;
        }
        let exchange = col_of(&headers, row, "exchange").unwrap_or_default();
        let plain_json = legacy_row_credentials_json(cipher, &name, &headers, row)?;
        // 整串归一化为密文（无 key 时保留明文，由调用方口径负责告警）。
        let stored = match cipher {
            Some(c) => c.encrypt_if_plaintext(&plain_json)?.0,
            None => plain_json,
        };
        let updated_at = col_of(&headers, row, "updated_at")
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(0);
        new_rows.push(
            build_account_row(
                &ACCOUNT_CSV_HEADER,
                &AccountRowInput {
                    name: &name,
                    exchange: &exchange,
                    credentials: &stored,
                    updated_at,
                },
            )
            .map_err(anyhow::Error::msg)?,
        );
    }

    // 备份原文件 → 原子回写新 7 列 CSV。
    let backup = path.with_extension("csv.bak");
    std::fs::copy(path, &backup).with_context(|| format!("备份原文件失败 {}", path.display()))?;
    let mut writer = csv::Writer::from_writer(Vec::new());
    writer.write_record(ACCOUNT_CSV_HEADER.iter())?;
    for row in &new_rows {
        writer.write_record(row.iter())?;
    }
    write_csv_atomic(path, &writer.into_inner()?)?;
    Ok(MigrateReport {
        migrated: new_rows.len(),
        backup: Some(backup),
    })
}

/// 按表头名取某列值（trim 后）。
fn col_of(headers: &csv::StringRecord, row: &[String], col: &str) -> Option<String> {
    headers
        .iter()
        .position(|h| h.trim() == col)
        .and_then(|i| row.get(i))
        .map(|v| v.trim().to_string())
}

/// 旧宽表行 → credentials JSON 明文（migrate 专用；映射口径与原 store 兼容层
/// 一致）。CTP 行：`api_key`=user_id、`api_passphrase`=password（`api_secret`
/// 空置）；REST 所三列直取。旧 `api_secret` 列若已是 `enc.v1.` 密文，
/// 无主密钥时报错——JSON 内不存密文，整串由调用方统一加密。
fn legacy_row_credentials_json(
    cipher: Option<&SecretCipher>,
    name: &str,
    headers: &csv::StringRecord,
    row: &[String],
) -> anyhow::Result<String> {
    let val = |k: &str| col_of(headers, row, k).unwrap_or_default();
    if val("exchange").trim() == "ctp" {
        let creds = trading_exchange::port::CtpCreds {
            broker_id: val("broker_id"),
            user_id: val("api_key"),
            password: val("api_passphrase"),
            app_id: val("app_id"),
            auth_code: val("auth_code"),
            investor_id: val("investor_id"),
            front_address: val("front_address"),
            is_production_mode: legacy_parse_bool(&val("ctp_production_mode")),
        };
        Ok(serde_json::to_string(&creds)?)
    } else {
        let raw_secret = val("api_secret");
        let api_secret = match cipher {
            Some(c) => c.decrypt(&raw_secret)?,
            None if raw_secret.starts_with(trading_models::account::SECRET_PREFIX) => {
                anyhow::bail!("账户 `{name}` 的旧 api_secret 列已是密文：迁移需要 --key 提供主密钥")
            }
            None => raw_secret,
        };
        let passphrase = val("api_passphrase");
        let passphrase = passphrase.trim();
        let creds = trading_exchange::port::ApiSecretCreds {
            api_key: val("api_key"),
            api_secret,
            api_passphrase: (!passphrase.is_empty()).then(|| passphrase.to_string()),
            position_mode: legacy_parse_position_mode(&val("position_mode")),
            is_testnet: legacy_parse_bool(&val("is_testnet")),
        };
        Ok(serde_json::to_string(&creds)?)
    }
}

/// 旧宽表 position_mode 列宽松解析（与原 store 兼容层同口径）；非法/缺省 one_way。
fn legacy_parse_position_mode(raw: &str) -> trading_models::PositionMode {
    match raw.trim().to_ascii_lowercase().as_str() {
        "hedge" | "dual" | "true" | "1" => trading_models::PositionMode::Hedge,
        _ => trading_models::PositionMode::OneWay,
    }
}

/// 旧宽表布尔列解析：1/true 为真，其余为假。
fn legacy_parse_bool(raw: &str) -> bool {
    matches!(raw.trim().to_ascii_lowercase().as_str(), "1" | "true")
}

/// add-account 的行输入（集中传参，便于纯函数测试）。
struct AccountRowInput<'a> {
    name: &'a str,
    exchange: &'a str,
    /// credentials 整串（密文 `enc.v1.*` 或联调明文 JSON）。
    credentials: &'a str,
    updated_at: u64,
}

/// 按表头列序组装一条账户行；身份字段为空即报错（服务端同样会拒绝）。
fn build_account_row(headers: &[&str], input: &AccountRowInput<'_>) -> Result<Vec<String>, String> {
    if input.name.trim().is_empty() {
        return Err("账户名（--name）不能为空".to_string());
    }
    if input.exchange.trim().is_empty() {
        return Err("交易所（--exchange）不能为空".to_string());
    }
    if input.credentials.trim().is_empty() {
        return Err("credentials JSON 不能为空".to_string());
    }
    let mut row = vec![String::new(); headers.len()];
    for (i, h) in headers.iter().enumerate() {
        row[i] = match *h {
            "id" => "0".to_string(),
            "name" => input.name.trim().to_string(),
            "exchange" => input.exchange.trim().to_string(),
            "credentials" => input.credentials.to_string(),
            "deleted" => "0".to_string(),
            "created_at" => "0".to_string(),
            "updated_at" => input.updated_at.to_string(),
            _other => String::new(), // 未知列留空，兼容扩展表头
        };
    }
    Ok(row)
}

/// 读 CSV 返回 (表头, 行集合)；文件不存在时用标准表头建空表。
fn read_csv_or_header(
    path: &Path,
    default_header: &[&str],
) -> anyhow::Result<(csv::StringRecord, Vec<Vec<String>>)> {
    if !path.exists() {
        return Ok((csv::StringRecord::from(default_header.to_vec()), Vec::new()));
    }
    let raw = std::fs::read_to_string(path)?;
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .from_reader(raw.as_bytes());
    let headers = reader.headers()?.clone();
    let mut rows = Vec::new();
    for rec in reader.records() {
        rows.push(rec?.iter().map(str::to_string).collect());
    }
    Ok((headers, rows))
}

/// 按 key 列 upsert：存在同名行时 `update=true` 整行替换（返回 true），
/// 否则报错；不存在则追加（返回 false）。
fn upsert_row(
    headers: &csv::StringRecord,
    rows: &mut Vec<Vec<String>>,
    record: &[String],
    key_column: &str,
    update: bool,
) -> anyhow::Result<bool> {
    let key_idx = headers
        .iter()
        .position(|h| h.trim() == key_column)
        .ok_or_else(|| anyhow::anyhow!("CSV 缺少 {key_column} 列"))?;
    let key = &record[key_idx];
    if let Some(pos) = rows
        .iter()
        .position(|r| r.get(key_idx).map(String::as_str) == Some(key))
    {
        if !update {
            anyhow::bail!("账户 `{key}` 已存在；如需覆盖请加 --update（将整行替换）");
        }
        rows[pos] = record.to_vec();
        return Ok(true);
    }
    rows.push(record.to_vec());
    Ok(false)
}

/// 原子回写：tmp + fsync + rename；新建文件收紧为 0600（内容含私钥）。
fn write_csv_atomic(path: &Path, bytes: &[u8]) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }
    let tmp = path.with_extension("csv.tmp");
    {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perm = std::fs::metadata(path)?.permissions();
        perm.set_mode(0o600);
        std::fs::set_permissions(path, perm)?;
    }
    Ok(())
}

/// 读取 32 字节主密钥并构造加解密器。
fn load_cipher(path: &Path) -> anyhow::Result<SecretCipher> {
    let bytes = std::fs::read(path)
        .map_err(|e| anyhow::anyhow!("读取主密钥 {} 失败：{e}", path.display()))?;
    SecretCipher::from_key_bytes(&bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    const HEADER: [&str; 7] = ACCOUNT_CSV_HEADER;

    #[test]
    fn default_data_dir_priority() {
        // 本机存在 /opt 布局（本会话所在部署机）或不存在，两种基线都可能——
        // 分别验证 env 覆盖与回退逻辑，不做绝对断言。
        // 1) 显式环境变量永远最高。
        std::env::set_var("TRADING_DATA_DIR", "/tmp/some/dir");
        assert_eq!(default_data_dir(), "/tmp/some/dir");
        // 2) 空串视为未设置，落到后续分支。
        std::env::set_var("TRADING_DATA_DIR", "  ");
        let fallback = if std::path::Path::new(OPT_DATA_DIR).is_dir() {
            OPT_DATA_DIR
        } else {
            "./data"
        };
        assert_eq!(default_data_dir(), fallback);
        std::env::remove_var("TRADING_DATA_DIR");
        assert_eq!(default_data_dir(), fallback);
    }

    #[test]
    fn tool_header_matches_model_header() {
        // 工具写入的表头必须与服务端模型常量逐列一致。
        let model: Vec<&str> = trading_models::ACCOUNT_CSV_HEADER.split(',').collect();
        assert_eq!(HEADER.to_vec(), model);
    }

    fn sample_row(name: &str, credentials: &str) -> Vec<String> {
        build_account_row(
            &HEADER,
            &AccountRowInput {
                name,
                exchange: "binance",
                credentials,
                updated_at: 0,
            },
        )
        .unwrap()
    }

    #[test]
    fn row_matches_header_order() {
        let row = sample_row("acct_a", "enc.v1.x");
        assert_eq!(row.len(), HEADER.len());
        let get = |col: &str| {
            let i = HEADER.iter().position(|h| *h == col).unwrap();
            row[i].clone()
        };
        assert_eq!(get("name"), "acct_a");
        assert_eq!(get("exchange"), "binance");
        assert_eq!(get("credentials"), "enc.v1.x");
        assert_eq!(get("deleted"), "0");
        assert_eq!(get("id"), "0");
        assert_eq!(get("updated_at"), "0");
    }

    #[test]
    fn empty_name_exchange_or_credentials_rejected() {
        let err = build_account_row(
            &HEADER,
            &AccountRowInput {
                name: "  ",
                exchange: "binance",
                credentials: "enc.v1.x",
                updated_at: 0,
            },
        )
        .unwrap_err();
        assert!(err.contains("--name"));

        let err = build_account_row(
            &HEADER,
            &AccountRowInput {
                name: "a",
                exchange: "binance",
                credentials: "  ",
                updated_at: 0,
            },
        )
        .unwrap_err();
        assert!(err.contains("credentials"));
    }

    #[test]
    fn credentials_template_has_two_variants() {
        // CTP 与 REST 所模板各一段；CTP 模板含看穿式字段，REST 模板含 api_key。
        assert!(credentials_template("ctp").contains("broker_id"));
        assert!(credentials_template("CTP").contains("broker_id"));
        assert!(credentials_template("binance").contains("api_key"));
        assert!(credentials_template("deribit").contains("api_key"));
    }

    #[test]
    fn credentials_json_validated_before_write() {
        // 写入前按 exchange 做 schema 校验：合法 JSON 通过，非法 JSON/错 schema 报错。
        let ok = r#"{"api_key":"k","api_secret":"s"}"#;
        assert!(credential_from_json("binance", ok).is_ok());
        assert!(credential_from_json("binance", "not-json").is_err());
        let ctp_ok = r#"{"broker_id":"9999","user_id":"u","password":"p","app_id":"a","auth_code":"c","front_address":"tcp://f:1"}"#;
        assert!(credential_from_json("ctp", ctp_ok).is_ok());
        // CTP 行误用 REST schema → 报错（防错列写入）。
        assert!(credential_from_json("ctp", ok).is_err());
    }

    #[test]
    fn upsert_appends_new_and_rejects_duplicate() {
        let mut rows: Vec<Vec<String>> = vec![sample_row("acct_a", "enc.v1.old")];
        let headers = csv::StringRecord::from(HEADER.to_vec());

        // 新名字 → 追加。
        let replaced = upsert_row(
            &headers,
            &mut rows,
            &sample_row("acct_b", "enc.v1.x"),
            "name",
            false,
        )
        .unwrap();
        assert!(!replaced);
        assert_eq!(rows.len(), 2);

        // 同名且未开 --update → 报错，不动数据。
        assert!(upsert_row(
            &headers,
            &mut rows,
            &sample_row("acct_a", "enc.v1.new"),
            "name",
            false
        )
        .is_err());
        assert_eq!(rows[0][1], "acct_a");
        assert_eq!(rows[0][3], "enc.v1.old");

        // 同名 + --update → 整行替换。
        let replaced = upsert_row(
            &headers,
            &mut rows,
            &sample_row("acct_a", "enc.v1.new"),
            "name",
            true,
        )
        .unwrap();
        assert!(replaced);
        assert_eq!(rows[0][3], "enc.v1.new");
    }

    #[test]
    fn resolve_update_semantics() {
        let headers = csv::StringRecord::from(HEADER.to_vec());
        let rows = vec![sample_row("acct_a", "enc.v1.old")];

        // 非交互（测试环境 stdin 非 tty）+ 未开 update + 已存在 → 报错。
        assert!(resolve_update(&headers, &rows, "acct_a", false).is_err());
        // 开了 update / 不存在 → 直接通过。
        assert!(resolve_update(&headers, &rows, "acct_a", true).unwrap());
        assert!(!resolve_update(&headers, &rows, "acct_b", false).unwrap());
    }

    #[test]
    fn legacy_row_credentials_json_binance_maps_columns() {
        // Arrange：旧宽表 binance 行（明文 secret + hedge + 测试网 + passphrase）。
        let headers = csv::StringRecord::from(vec![
            "id",
            "name",
            "exchange",
            "api_key",
            "api_secret",
            "api_passphrase",
            "position_mode",
            "is_testnet",
            "deleted",
            "created_at",
            "updated_at",
        ]);
        let row: Vec<String> = [
            "0", "alice", "binance", "KEY-A", "secret-a", "pp", "hedge", "1", "0", "0", "0",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        let cipher = SecretCipher::from_key_bytes(&SecretCipher::random_key()).unwrap();

        // Act
        let json = legacy_row_credentials_json(Some(&cipher), "alice", &headers, &row).unwrap();

        // Assert：按 ApiSecretCreds schema 解析且字段逐一就位。
        let creds: trading_exchange::port::ApiSecretCreds = serde_json::from_str(&json).unwrap();
        assert_eq!(creds.api_key, "KEY-A");
        assert_eq!(creds.api_secret, "secret-a");
        assert_eq!(creds.api_passphrase.as_deref(), Some("pp"));
        assert_eq!(creds.position_mode, trading_models::PositionMode::Hedge);
        assert!(creds.is_testnet);
    }

    #[test]
    fn legacy_row_credentials_json_ctp_maps_columns() {
        // Arrange：旧宽表 CTP 全字段行（api_key=user_id、api_passphrase=password）。
        let headers = csv::StringRecord::from(vec![
            "id",
            "name",
            "exchange",
            "api_key",
            "api_secret",
            "api_passphrase",
            "position_mode",
            "is_testnet",
            "broker_id",
            "app_id",
            "auth_code",
            "investor_id",
            "front_address",
            "ctp_production_mode",
            "deleted",
            "created_at",
            "updated_at",
        ]);
        let row: Vec<String> = [
            "0",
            "ctp-acct",
            "ctp",
            "user999",
            "",
            "pwd123",
            "one_way",
            "0",
            "9999",
            "simnow_client",
            "0000000000000000",
            "",
            "tcp://182.254.243.31:40001",
            "1",
            "0",
            "0",
            "0",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();

        // Act
        let json = legacy_row_credentials_json(None, "ctp-acct", &headers, &row).unwrap();

        // Assert：按 CtpCreds schema 解析且看穿式字段就位。
        let creds: trading_exchange::port::CtpCreds = serde_json::from_str(&json).unwrap();
        assert_eq!(creds.broker_id, "9999");
        assert_eq!(creds.user_id, "user999");
        assert_eq!(creds.password, "pwd123");
        assert_eq!(creds.app_id, "simnow_client");
        assert_eq!(creds.auth_code, "0000000000000000");
        assert_eq!(creds.front_address, "tcp://182.254.243.31:40001");
        assert!(creds.is_production_mode);
    }

    #[test]
    fn legacy_row_encrypted_secret_requires_key() {
        // Arrange：旧 api_secret 列已是 enc.v1. 密文。
        let headers = csv::StringRecord::from(vec![
            "id",
            "name",
            "exchange",
            "api_key",
            "api_secret",
            "api_passphrase",
            "position_mode",
            "is_testnet",
            "deleted",
            "created_at",
            "updated_at",
        ]);
        let cipher = SecretCipher::from_key_bytes(&SecretCipher::random_key()).unwrap();
        let enc = cipher.encrypt("topsecret").unwrap();
        let mk_row = |secret: &str| -> Vec<String> {
            [
                "0", "carol", "binance", "K", secret, "", "one_way", "0", "0", "0", "0",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect()
        };

        // Act / Assert：无 --key → 报错要求主密钥；有 key → 解出明文。
        assert!(legacy_row_credentials_json(None, "carol", &headers, &mk_row(&enc)).is_err());
        let json =
            legacy_row_credentials_json(Some(&cipher), "carol", &headers, &mk_row(&enc)).unwrap();
        let creds: trading_exchange::port::ApiSecretCreds = serde_json::from_str(&json).unwrap();
        assert_eq!(creds.api_secret, "topsecret");
    }

    #[test]
    fn migrate_csv_rewrites_with_backup_and_roundtrip() {
        // Arrange：旧宽表 CSV（binance + deribit 两行），密钥已备。
        let dir = std::env::temp_dir().join(format!("acct_mig_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("account.csv");
        let cipher = SecretCipher::from_key_bytes(&SecretCipher::random_key()).unwrap();
        let old = "id,name,exchange,api_key,api_secret,api_passphrase,position_mode,is_testnet,deleted,created_at,updated_at\n\
                   0,alice,binance,KEY-A,secret-a,,hedge,1,0,0,0\n\
                   0,d Dex,deribit,DK,ds,,one_way,0,0,0,0\n";
        std::fs::write(&path, old).unwrap();

        // Act
        let report = migrate_csv(&path, Some(&cipher)).unwrap();

        // Assert：迁移 2 行 + 原文件完整备份 .bak。
        assert_eq!(report.migrated, 2);
        let backup = report.backup.expect("应有 .bak 备份");
        assert_eq!(std::fs::read_to_string(&backup).unwrap(), old);

        // 新文件为 7 列且 credentials 已整串加密，可解回原字段。
        let (headers, rows) = read_csv_or_header(&path, &HEADER).unwrap();
        assert_eq!(headers.len(), 7);
        assert_eq!(rows.len(), 2);
        let name_idx = headers.iter().position(|h| h.trim() == "name").unwrap();
        let cred_idx = headers
            .iter()
            .position(|h| h.trim() == "credentials")
            .unwrap();
        let mut names: Vec<&str> = rows.iter().map(|r| r[name_idx].as_str()).collect();
        names.sort();
        assert_eq!(names, vec!["alice", "d Dex"]);
        for row in &rows {
            let stored = &row[cred_idx];
            assert!(stored.starts_with(trading_models::account::SECRET_PREFIX));
            let plain = cipher.decrypt(stored).unwrap();
            serde_json::from_str::<trading_exchange::port::ApiSecretCreds>(&plain).unwrap();
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn migrate_csv_noop_for_new_format() {
        // Arrange：已是新 7 列格式的文件。
        let dir = std::env::temp_dir().join(format!("acct_mig_new_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("account.csv");
        let before = format!("{}\n0,alice,binance,enc.v1.x,0,0,0\n", HEADER.join(","));
        std::fs::write(&path, &before).unwrap();

        // Act
        let report = migrate_csv(&path, None).unwrap();

        // Assert：no-op（不迁移、不备份、原文件不动）。
        assert_eq!(report.migrated, 0);
        assert!(report.backup.is_none());
        assert_eq!(std::fs::read_to_string(&path).unwrap(), before);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn read_missing_csv_yields_default_header() {
        let dir = std::env::temp_dir().join(format!("acct_tool_test_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("missing.csv");
        let (headers, rows) = read_csv_or_header(&path, &HEADER).unwrap();
        assert_eq!(headers.iter().collect::<Vec<_>>(), HEADER.to_vec());
        assert!(rows.is_empty());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn write_then_read_roundtrip_and_upsert() {
        let dir = std::env::temp_dir().join(format!("acct_tool_rt_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("account.csv");

        let headers = csv::StringRecord::from(HEADER.to_vec());
        let mut writer = csv::Writer::from_writer(Vec::new());
        writer.write_record(headers.iter()).unwrap();
        // credentials 为含逗号/引号的 JSON（csv writer 自动引号转义）。
        let json_a = r#"{"api_key":"k1","api_secret":"s1"}"#;
        let json_b = r#"{"broker_id":"9999","user_id":"u","password":"p","app_id":"a","auth_code":"c","front_address":"tcp://f:1"}"#;
        for (n, c) in [
            ("a", format!("enc.v1.{json_a}")),
            ("b", format!("enc.v1.{json_b}")),
        ] {
            writer.write_record(sample_row(n, &c).iter()).unwrap();
        }
        write_csv_atomic(&path, &writer.into_inner().unwrap()).unwrap();

        // 回读 + upsert 更新 a。
        let (headers, mut rows) = read_csv_or_header(&path, &HEADER).unwrap();
        upsert_row(
            &headers,
            &mut rows,
            &sample_row("a", "enc.v1.updated"),
            "name",
            true,
        )
        .unwrap();
        assert_eq!(rows[0][3], "enc.v1.updated");
        assert_eq!(rows.len(), 2);

        // 权限 0600（unix）。
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600);
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
