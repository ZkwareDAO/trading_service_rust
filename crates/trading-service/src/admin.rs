//! 管理 API（预留）：运行期运维触发入口，挂在南向同一个本地 HTTP 服务上。
//!
//! 当前仅提供账户重载：
//! * `POST /admin/accounts/reload` —— 重新读取并 reconcile account.csv；
//!   成功后账户管理器递增重载体，交易所接入层的周期对账循环据此把变更账户的
//!   用户数据流连接停旧起新（改密/切测试网/新增/禁用均无需重启进程）。
//!
//! 设计说明：本服务不做文件轮询热加载，重载只由显式触发；连接的实际
//! 增删改由交易所接入层的共享状态对账完成，handler 不直接操作连接。
//! 默认只绑定本地回环地址，鉴权（管理令牌）在后续迭代补上。

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Serialize;

use crate::state::AppState;

/// 统一应答信封：success + data(accounts) + error。
#[derive(Debug, Serialize)]
struct ReloadReply {
    success: bool,
    /// 本次 reconcile 写入的账户行数。
    accounts: usize,
    error: Option<String>,
}

/// `POST /admin/accounts/reload`：触发账户配置重载。
pub async fn reload_accounts(State(state): State<AppState>) -> Response {
    match state.accounts.reload() {
        Ok(n) => {
            tracing::info!(
                accounts = n,
                "账户重载成功（管理 API 触发，连接将在对账周期重建）"
            );
            (
                StatusCode::OK,
                Json(ReloadReply {
                    success: true,
                    accounts: n,
                    error: None,
                }),
            )
                .into_response()
        }
        Err(e) => {
            // 解析失败时账户管理器保留旧表（不半更新），返回 5xx 与原因。
            tracing::error!(error = %e, "账户重载失败，保留当前内存账户表");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ReloadReply {
                    success: false,
                    accounts: 0,
                    error: Some(e.to_string()),
                }),
            )
                .into_response()
        }
    }
}

// —— CTP 合约规格每日刷新（feature="ctp"；外部 cron 14:55 调本接口）——
//
// 阻塞的柜台短会话（登录 + 多轮 1 秒流控查询，全市场约数十秒）放在
// `spawn_blocking`，不占用 tokio worker；exchange 层只负责拉取与重灌内存目录，
// 落盘（store）与日期口径（宿主）经闭包注入，保持 exchange 不依赖 trading-store。

#[cfg(feature = "ctp")]
mod ctp_specs_refresh {
    use axum::extract::State;
    use axum::http::StatusCode;
    use axum::response::{IntoResponse, Response};
    use axum::Json;
    use serde::Serialize;
    use trading_exchange::ctp::provider::SpecRefreshBusy;
    use trading_exchange::ctp::spec_fetch::{FetchedCommission, FetchedInstrument, SpecSnapshot};
    use trading_store::ctp_specs::{CommissionSpecRow, InstrumentSpecRow};

    use crate::state::AppState;

    /// 统一应答信封：success + 三表条数 + error（与 reload_accounts 同口径）。
    #[derive(Debug, Serialize)]
    pub(super) struct RefreshSpecsReply {
        success: bool,
        /// 全量合约条数（含期权/组合）。
        instruments: usize,
        /// 其中在交易期货条数。
        futures: usize,
        /// 投资者手续费率条数。
        commissions: usize,
        error: Option<String>,
    }

    /// `POST /admin/ctp/refresh-specs`：拉取当日全量合约规格/手续费 → 落盘
    /// 日期文件 + latest 副本 → 原地重灌共享合约目录。
    ///
    /// 缺省用 venue.toml `[ctp].md_account` 的账户；已有刷新在跑时回 409（不排队）。
    pub(crate) async fn refresh_ctp_specs(State(state): State<AppState>) -> Response {
        // CTP provider 具体句柄（未启用 ctp / 自定义装配为 None）。
        let provider = match state.exchanges.ctp_provider() {
            Some(p) => p,
            None => {
                return reply(
                    StatusCode::SERVICE_UNAVAILABLE,
                    err_reply("当前装配未启用 CTP provider".into()),
                );
            }
        };
        let specs_store = state.store.ctp_specs();
        // 交易日取本地日期（外部 cron 在 14:55 调用，即当日）。
        let day = chrono::Local::now().format("%Y-%m-%d").to_string();

        // 阻塞拉取 + 落盘 + 重灌（数十秒），不阻塞异步 worker。
        // 落盘日期由闭包内的 day 副本掌握（exchange 层不收日期参数）。
        let result = tokio::task::spawn_blocking({
            let day = day.clone();
            move || {
                provider
                    .refresh_specs(move |snapshot| persist_snapshot(&specs_store, &day, snapshot))
            }
        })
        .await;

        match result {
            // panic/取消（守卫已在 unwind 时释放刷新闸门）。
            Err(join) => reply(
                StatusCode::INTERNAL_SERVER_ERROR,
                err_reply(format!("规格刷新任务异常终止：{join}")),
            ),
            Ok(Ok(counts)) => {
                tracing::info!(?counts, day = %day, "CTP 合约规格刷新成功（管理 API 触发）");
                reply(
                    StatusCode::OK,
                    RefreshSpecsReply {
                        success: true,
                        instruments: counts.instruments,
                        futures: counts.futures,
                        commissions: counts.commissions,
                        error: None,
                    },
                )
            }
            Ok(Err(e)) => {
                // 并发刷新冲突 → 409；其余（无账户/柜台失败/落盘失败）→ 500。
                let status = if e.downcast_ref::<SpecRefreshBusy>().is_some() {
                    StatusCode::CONFLICT
                } else {
                    StatusCode::INTERNAL_SERVER_ERROR
                };
                tracing::error!(error = %e, "CTP 合约规格刷新失败");
                reply(status, err_reply(e.to_string()))
            }
        }
    }

    fn reply(status: StatusCode, body: RefreshSpecsReply) -> Response {
        (status, Json(body)).into_response()
    }

    fn err_reply(error: String) -> RefreshSpecsReply {
        RefreshSpecsReply {
            success: false,
            instruments: 0,
            futures: 0,
            commissions: 0,
            error: Some(error),
        }
    }

    /// 把拉取快照转成 store 行并写日期文件 + latest（exchange 行 → store 行的
    /// 字段映射只在这一宿主层做，避免任一下层反向依赖对方）。
    fn persist_snapshot(
        store: &trading_store::ctp_specs::CtpSpecsStore,
        day: &str,
        snapshot: &SpecSnapshot,
    ) -> anyhow::Result<()> {
        let instruments: Vec<InstrumentSpecRow> =
            snapshot.instruments.iter().map(to_instrument_row).collect();
        let commissions: Vec<CommissionSpecRow> =
            snapshot.commissions.iter().map(to_commission_row).collect();
        store.save_instruments(day, &instruments)?;
        store.save_commissions(day, &commissions)?;
        Ok(())
    }

    /// exchange 合约行 → store 合约行（逐字段同名映射）。
    fn to_instrument_row(i: &FetchedInstrument) -> InstrumentSpecRow {
        InstrumentSpecRow {
            instrument_id: i.instrument_id.clone(),
            exchange_id: i.exchange_id.clone(),
            product_id: i.product_id.clone(),
            instrument_name: i.instrument_name.clone(),
            product_class: i.product_class,
            volume_multiple: i.volume_multiple,
            price_tick: i.price_tick,
            min_limit_volume: i.min_limit_volume,
            max_limit_volume: i.max_limit_volume,
            min_market_volume: i.min_market_volume,
            max_market_volume: i.max_market_volume,
            long_margin_ratio: i.long_margin_ratio,
            short_margin_ratio: i.short_margin_ratio,
            is_trading: i.is_trading,
            open_date: i.open_date.clone(),
            expire_date: i.expire_date.clone(),
        }
    }

    /// exchange 手续费行 → store 手续费行（逐字段同名映射）。
    fn to_commission_row(c: &FetchedCommission) -> CommissionSpecRow {
        CommissionSpecRow {
            instrument_id: c.instrument_id.clone(),
            exchange_id: c.exchange_id.clone(),
            open_by_money: c.open_by_money,
            open_by_volume: c.open_by_volume,
            close_by_money: c.close_by_money,
            close_by_volume: c.close_by_volume,
            close_today_by_money: c.close_today_by_money,
            close_today_by_volume: c.close_today_by_volume,
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        fn snapshot() -> SpecSnapshot {
            SpecSnapshot {
                instruments: vec![FetchedInstrument {
                    instrument_id: "cu2611".into(),
                    exchange_id: "SHFE".into(),
                    product_id: "cu".into(),
                    instrument_name: "沪铜2611".into(),
                    product_class: b'1',
                    volume_multiple: 5,
                    price_tick: 10.0,
                    min_limit_volume: 1,
                    max_limit_volume: 500,
                    min_market_volume: 1,
                    max_market_volume: 60,
                    long_margin_ratio: 0.1,
                    short_margin_ratio: 0.1,
                    is_trading: true,
                    open_date: "20251118".into(),
                    expire_date: "20261116".into(),
                }],
                commissions: vec![FetchedCommission {
                    instrument_id: "cu2611".into(),
                    exchange_id: "SHFE".into(),
                    open_by_money: 0.0,
                    open_by_volume: 3.0,
                    close_by_money: 0.0,
                    close_by_volume: 3.0,
                    close_today_by_money: 0.0,
                    close_today_by_volume: 0.0,
                }],
            }
        }

        #[test]
        fn reply_envelope_shape() {
            let ok = serde_json::to_value(RefreshSpecsReply {
                success: true,
                instruments: 3000,
                futures: 863,
                commissions: 900,
                error: None,
            })
            .unwrap();
            assert_eq!(ok["success"], true);
            assert_eq!(ok["instruments"], 3000);
            assert_eq!(ok["futures"], 863);
            assert_eq!(ok["commissions"], 900);
            assert!(ok["error"].is_null());
        }

        #[test]
        fn mapping_preserves_fields() {
            let snap = snapshot();
            let i = to_instrument_row(&snap.instruments[0]);
            assert_eq!(i.instrument_id, "cu2611");
            assert_eq!(i.product_class, b'1');
            assert_eq!(i.volume_multiple, 5);
            assert!(i.is_trading);
            assert_eq!(i.expire_date, "20261116");
            let c = to_commission_row(&snap.commissions[0]);
            assert_eq!(c.open_by_volume, 3.0);
            assert_eq!(c.close_today_by_volume, 0.0);
        }

        #[test]
        fn persist_writes_dated_and_latest_files() {
            // Arrange：临时数据根 + 一份合成快照。
            let dir = tempfile::tempdir().unwrap();
            let store = trading_store::ctp_specs::CtpSpecsStore::new(dir.path().to_path_buf());

            // Act：宿主落盘。
            persist_snapshot(&store, "2026-10-26", &snapshot()).expect("persist ok");

            // Assert：日期文件与 latest 副本都写出，且内容含该合约/手续费。
            assert!(dir
                .path()
                .join("ctp-specs/202610/instruments-2026-10-26.csv")
                .exists());
            assert!(dir
                .path()
                .join("ctp-specs/202610/commissions-2026-10-26.csv")
                .exists());
            let latest_inst =
                std::fs::read_to_string(dir.path().join("ctp-specs/latest/instruments.csv"))
                    .unwrap();
            assert!(latest_inst.contains("cu2611"));
            let latest_comm =
                std::fs::read_to_string(dir.path().join("ctp-specs/latest/commissions.csv"))
                    .unwrap();
            assert!(latest_comm.contains("cu2611"));
        }
    }
}

#[cfg(feature = "ctp")]
pub(crate) use ctp_specs_refresh::refresh_ctp_specs;

#[cfg(test)]
mod tests {
    use super::*;

    /// 信封序列化字段钉死：success/accounts/error 三段式。
    #[test]
    fn reply_envelope_shape() {
        let ok = serde_json::to_value(ReloadReply {
            success: true,
            accounts: 2,
            error: None,
        })
        .unwrap();
        assert_eq!(ok["success"], true);
        assert_eq!(ok["accounts"], 2);
        assert!(ok["error"].is_null());
    }
}
