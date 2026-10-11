//! 账户凭证加密 / 账户管理小工具（生产前置）。
//!
//! 复用与服务完全一致的 AES-256-GCM 原语（`trading_models::SecretCipher`）：
//! * `gen-key`：生成 32 字节主密钥，0600 落盘（即 account.key）
//! * `encrypt`：加密单个明文，输出 `enc.v1.*`
//! * `encrypt-csv`：把 account.csv 中 api_secret 列的明文批量加密、原子回写
//! * `add-account`：校验并追加一条账户到 account.csv（文件不存在先建表头；
//!   同名账户默认拒绝，`--update` 时按 name 整行替换），`--key` 提供时写入前
//!   即刻加密 secret
//!
//! 已是 `enc.v1.*` / 空值会原样跳过（encrypt/encrypt-csv）。
//!
//! # 交互式与脚本两种用法
//!
//! 所有参数都是**可选**的：交互终端下缺什么就问什么（secret 输入不回显）；
//! 非交互管道/脚本场景则按命令行参数执行，必填参数缺失时报错退出，方便
//! CI / 部署脚本无人工调用。
//!
//! 注意：account.key 存的是 32 字节原始密钥（非 hex），与服务读取口径一致。
//! 主密钥丢失后历史密文将无法解密，请妥善备份。
//!
//! CSV 口径（与 trading-store 的读取一致）：服务按 `name` 作为账户身份（id 由
//! 服务端按 name 稳定哈希生成，CSV 中的 id/created_at 列不参与加载），因此本
//! 工具写入 id=0、created_at=0 即可；`updated_at` 由调用方按需携带（缺省 0）。

use std::io::{IsTerminal, Read, Write};
use std::path::{Path, PathBuf};

use clap::{Parser, Subcommand};
use trading_models::account::SecretCipher;

/// account.csv 的标准表头（与 deploy/account.example.csv 一致）。
pub const ACCOUNT_CSV_HEADER: [&str; 11] = [
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

    /// 批量加密 account.csv 的 api_secret 列，原地原子回写。
    EncryptCsv {
        /// 主密钥文件（32 字节 raw）。
        #[arg(long)]
        key: Option<PathBuf>,
        /// account.csv 路径。
        #[arg(long)]
        csv: Option<PathBuf>,
    },

    /// 追加一条账户到 account.csv（文件不存在则先创建表头）。
    ///
    /// 交互终端下逐项提问（secret 不回显）；脚本场景用参数直传。
    AddAccount {
        /// account.csv 路径（数据目录下，如 ./data/account.csv）。
        #[arg(long)]
        csv: Option<PathBuf>,
        /// 账户名（同一 exchange 内唯一；服务据此生成稳定 id）。
        #[arg(long)]
        name: Option<String>,
        /// 交易所精确名（binance / deribit / hyperliquid …）。
        #[arg(long)]
        exchange: Option<String>,
        /// API key / 公钥。
        #[arg(long)]
        api_key: Option<String>,
        /// API secret / 私钥；不传则交互提问（不回显）或从 stdin 读取。
        #[arg(long)]
        secret: Option<String>,
        /// 主密钥文件；提供时写入前先加密为 enc.v1.*（生产推荐）。
        /// 缺省则按明文写入（服务启动会告警，仅限本地联调）。
        #[arg(long)]
        key: Option<PathBuf>,
        /// 第三凭证/口令（部分交易所使用；binance/deribit/hyperliquid 为空）。
        #[arg(long)]
        api_passphrase: Option<String>,
        /// 持仓模式：one_way（默认）/ hedge。
        #[arg(long)]
        position_mode: Option<String>,
        /// 连接交易所测试网（生产必须省略此开关）。
        #[arg(long, default_value_t = false)]
        testnet: bool,
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
            println!("请妥善备份；丢失后已加密的 api_secret 无法解密。");
        }
        Cmd::Encrypt { key, secret } => {
            let key = match key {
                Some(k) => k,
                None => prompt_path("主密钥文件路径", &default_data_file("account.key"))?,
            };
            let cipher = load_cipher(&key)?;
            let plain = read_secret(secret)?;
            println!("{}", cipher.encrypt(&plain)?);
        }

        Cmd::EncryptCsv { key, csv } => {
            let key = match key {
                Some(k) => k,
                None => prompt_path("主密钥文件路径", &default_data_file("account.key"))?,
            };
            let csv = match csv {
                Some(c) => c,
                None => prompt_path("account.csv 路径", &default_data_file("account.csv"))?,
            };
            let cipher = load_cipher(&key)?;
            let raw = std::fs::read_to_string(&csv)?;
            let mut reader = csv::ReaderBuilder::new()
                .has_headers(true)
                .flexible(true)
                .from_reader(raw.as_bytes());
            let headers = reader.headers()?.clone();

            let secret_idx = headers
                .iter()
                .position(|h| h.trim() == "api_secret")
                .ok_or_else(|| anyhow::anyhow!("CSV 缺少 api_secret 列"))?;

            let mut writer = csv::Writer::from_writer(Vec::new());
            writer.write_record(headers.iter())?;

            let mut encrypted = 0usize;
            for record in reader.records() {
                let mut record = record?;
                if let Some(field) = record.get(secret_idx) {
                    let value = field.trim();
                    if !value.is_empty()
                        && !value.starts_with(trading_models::account::SECRET_PREFIX)
                    {
                        let enc = cipher.encrypt(value)?;
                        // StringRecord 无原地替换：按列重建该记录。
                        record = csv::StringRecord::from(
                            record
                                .iter()
                                .enumerate()
                                .map(|(i, v)| if i == secret_idx { enc.as_str() } else { v })
                                .collect::<Vec<_>>(),
                        );
                        encrypted += 1;
                    }
                }
                writer.write_record(record.iter())?;
            }
            let bytes = writer.into_inner()?;

            // 原子回写：tmp + rename，避免写一半损坏 account.csv。
            write_csv_atomic(&csv, &bytes)?;
            println!("已加密 {encrypted} 条明文 api_secret：{}", csv.display());
        }

        Cmd::AddAccount {
            csv,
            name,
            exchange,
            api_key,
            secret,
            key,
            api_passphrase,
            position_mode,
            testnet,
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
                None => prompt_required("交易所（binance / deribit / hyperliquid …）")?,
            };
            let api_key = match api_key {
                Some(k) => k,
                None => prompt_required("API key / 公钥")?,
            };
            let plain = read_secret(secret)?;
            if plain.is_empty() {
                anyhow::bail!("secret 为空：请用 --secret 传入、交互输入或从 stdin 提供私钥");
            }
            let key = match key {
                Some(k) => Some(k),
                None if interactive() => {
                    prompt_path_opt("主密钥路径（回车跳过 = 明文写入，仅限联调）")?
                }
                None => None,
            };
            let api_passphrase = match api_passphrase {
                Some(p) => Some(p),
                None => prompt_optional("第三凭证/口令 passphrase（多数交易所为空，回车跳过）")?,
            };
            let position_mode = match position_mode {
                Some(p) => p,
                None => prompt_default("持仓模式（one_way / hedge）", "one_way")?,
            };
            // 测试网开关：参数未给时交互确认（生产默认 N，防止误把测试网 key 写成主网口径）。
            let testnet = if testnet {
                true
            } else if interactive() {
                prompt_yes_no("是否连接交易所测试网（testnet）？", false)?
            } else {
                false
            };

            // 主密钥提供 → 写入前即刻加密（生产路径）；缺省按明文（联调，服务会告警）。
            let stored_secret = match &key {
                Some(k) => load_cipher(k)?.encrypt(&plain)?,
                None => {
                    eprintln!(
                        "警告：未提供主密钥，secret 将以明文写入（仅限本地联调，服务启动会告警）。"
                    );
                    plain
                }
            };

            let mode = normalize_position_mode(&position_mode).ok_or_else(|| {
                anyhow::anyhow!("非法 position_mode `{position_mode}`（仅支持 one_way / hedge）")
            })?;

            // 组装行（列序与表头一致）；id/created_at 写 0——服务按 name 生成稳定 id。
            let record = build_account_row(
                &ACCOUNT_CSV_HEADER,
                &AccountRowInput {
                    name: &name,
                    exchange: &exchange,
                    api_key: &api_key,
                    api_secret: &stored_secret,
                    api_passphrase: api_passphrase.as_deref(),
                    position_mode: mode,
                    is_testnet: testnet,
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

/// 提问读一个路径（带默认值）。
fn prompt_path(label: &str, default: &str) -> anyhow::Result<PathBuf> {
    Ok(PathBuf::from(prompt_default(label, default)?))
}

/// 提问读一个可选路径；空输入返回 None。
fn prompt_path_opt(label: &str) -> anyhow::Result<Option<PathBuf>> {
    Ok(prompt_optional(label)?.map(PathBuf::from))
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

/// secret 入参兜底：交互 → 不回显提问；管道 → 读 stdin（去除首尾空白）。
fn read_secret(secret: Option<String>) -> anyhow::Result<String> {
    if let Some(s) = secret {
        return Ok(s.trim().to_string());
    }
    if interactive() {
        // 不回显输入，防止私钥留在终端回滚缓冲/屏幕记录里。
        Ok(
            rpassword::prompt_password("API secret / 私钥（输入不回显）：")?
                .trim()
                .to_string(),
        )
    } else {
        let mut buf = String::new();
        std::io::stdin().read_to_string(&mut buf)?;
        Ok(buf.trim().to_string())
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

/// add-account 的行输入（集中传参，便于纯函数测试）。
struct AccountRowInput<'a> {
    name: &'a str,
    exchange: &'a str,
    api_key: &'a str,
    api_secret: &'a str,
    api_passphrase: Option<&'a str>,
    position_mode: &'a str,
    is_testnet: bool,
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
    let mut row = vec![String::new(); headers.len()];
    for (i, h) in headers.iter().enumerate() {
        row[i] = match *h {
            "id" => "0".to_string(),
            "name" => input.name.trim().to_string(),
            "exchange" => input.exchange.trim().to_string(),
            "api_key" => input.api_key.trim().to_string(),
            "api_secret" => input.api_secret.to_string(),
            "api_passphrase" => input.api_passphrase.unwrap_or("").to_string(),
            "position_mode" => input.position_mode.to_string(),
            "is_testnet" => (input.is_testnet as u8).to_string(),
            "deleted" => "0".to_string(),
            "created_at" => "0".to_string(),
            "updated_at" => input.updated_at.to_string(),
            _other => String::new(), // 未知列留空，兼容扩展表头
        };
    }
    Ok(row)
}

/// position_mode 宽松归一（与服务端 parse_position_mode 同口径）。
fn normalize_position_mode(raw: &str) -> Option<&'static str> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "one_way" | "oneway" | "one-way" | "" => Some("one_way"),
        "hedge" | "dual" => Some("hedge"),
        _ => None,
    }
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

    const HEADER: [&str; 11] = ACCOUNT_CSV_HEADER;

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

    fn sample_row(name: &str, secret: &str) -> Vec<String> {
        build_account_row(
            &HEADER,
            &AccountRowInput {
                name,
                exchange: "binance",
                api_key: "KEY",
                api_secret: secret,
                api_passphrase: None,
                position_mode: "one_way",
                is_testnet: false,
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
        assert_eq!(get("api_secret"), "enc.v1.x");
        assert_eq!(get("is_testnet"), "0");
        assert_eq!(get("deleted"), "0");
        assert_eq!(get("id"), "0");
        assert_eq!(get("position_mode"), "one_way");
    }

    #[test]
    fn empty_name_or_exchange_rejected() {
        let err = build_account_row(
            &HEADER,
            &AccountRowInput {
                name: "  ",
                exchange: "binance",
                api_key: "K",
                api_secret: "s",
                api_passphrase: None,
                position_mode: "one_way",
                is_testnet: false,
                updated_at: 0,
            },
        )
        .unwrap_err();
        assert!(err.contains("--name"));
    }

    #[test]
    fn position_mode_normalization() {
        assert_eq!(normalize_position_mode("HEDGE"), Some("hedge"));
        assert_eq!(normalize_position_mode(" dual "), Some("hedge"));
        assert_eq!(normalize_position_mode(""), Some("one_way"));
        assert_eq!(normalize_position_mode("xxx"), None);
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
        assert_eq!(rows[0][4], "enc.v1.old");

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
        assert_eq!(rows[0][4], "enc.v1.new");
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
    fn read_missing_csv_yields_default_header() {
        let dir = std::env::temp_dir().join(format!("acct_tool_test_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("missing.csv");
        let (headers, rows) = read_csv_or_header(&path, &HEADER).unwrap();
        assert_eq!(
            headers.iter().collect::<Vec<_>>(),
            HEADER.iter().copied().collect::<Vec<_>>()
        );
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
        for r in [sample_row("a", "enc.v1.1"), sample_row("b", "enc.v1.2")] {
            writer.write_record(r.iter()).unwrap();
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
        assert_eq!(rows[0][4], "enc.v1.updated");
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
