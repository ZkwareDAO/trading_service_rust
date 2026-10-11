//! 仓位对账去抖器（跨交易所通用的纯决策内核，零网络/时钟注入）。
//!
//! 对账原则（见 `docs/扩展交易所.md` 第五节）：WS 账户事件只当"仓位脏了"信号，
//! **真值一律走 REST `get_positions` 拉取**，以覆盖外部导入仓位、强平、资金费等不产生
//! 本地订单事件的变动，并避免 WS/REST 两条解析路径漂移。
//!
//! 触发时机：启动全量 + 账户事件去抖触发 + 定时全量兜底 + 重连补一次。去抖把一个窗口内
//! 的多次账户事件合并成一次 REST 拉取；定时器保证漏掉信号时最终一致。
//!
//! 纯决策内核已由单测覆盖；消费 [`crate::api::AccountEventStream`] 事件并回调
//! `get_positions` 拉真值、写仓的对账驱动器见 `binance/user_stream.rs`。

use std::collections::BTreeSet;

use crate::api::AccountDirty;

/// 去抖窗口结束后要执行的对账动作。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReconcileAction {
    /// 全量拉取所有持仓（启动/定时/重连/无具体标的的账户事件）。
    Full,
    /// 窗口内仅这些标的变脏；IO 层可定向查，也可直接退化为一次全量 REST。
    Dirty(Vec<String>),
}

/// 去抖/定时决策器。所有时间由调用方注入毫秒时间戳，便于确定性单测。
#[derive(Debug, Clone)]
pub struct ReconcileDebouncer {
    debounce_window_ms: i64,
    full_interval_ms: i64,
    /// 窗口内累积的脏标的（有序去重）。
    pending: BTreeSet<String>,
    /// 收到无标的账户事件（余额变动等）→ 需要全量。
    pending_full: bool,
    /// 本窗口首个脏信号时间；None 表示无待处理。
    first_dirty_ms: Option<i64>,
    /// 上次全量时间；None 表示尚未全量（启动即触发）。
    last_full_ms: Option<i64>,
    /// 重连等外部强制全量请求。
    force_full: bool,
}

impl ReconcileDebouncer {
    /// `debounce_window_ms`：脏信号合并窗口；`full_interval_ms`：定时全量兜底周期。
    pub fn new(debounce_window_ms: i64, full_interval_ms: i64) -> Self {
        Self {
            debounce_window_ms: debounce_window_ms.max(1),
            full_interval_ms: full_interval_ms.max(1),
            pending: BTreeSet::new(),
            pending_full: false,
            first_dirty_ms: None,
            last_full_ms: None,
            force_full: false,
        }
    }

    /// 记一个账户脏信号。空标的列表视为需要全量对账。
    pub fn note_dirty(&mut self, dirty: &AccountDirty, now_ms: i64) {
        if dirty.instruments.is_empty() {
            self.pending_full = true;
        } else {
            for s in &dirty.instruments {
                self.pending.insert(s.clone());
            }
        }
        if self.first_dirty_ms.is_none() {
            self.first_dirty_ms = Some(now_ms);
        }
    }

    /// 重连后调用：补一次全量（强平/漏单在断线期间也能被覆盖）。
    pub fn note_reconnected(&mut self) {
        self.force_full = true;
    }

    /// 每次事件循环驱动；返回此刻应执行的对账动作（最多一个），由 IO 层去拉 REST。
    pub fn poll(&mut self, now_ms: i64) -> Option<ReconcileAction> {
        // 1) 强制（重连）或启动/定时到点：全量优先，且全量已涵盖所有挂起的脏标的。
        let due_full = self.force_full
            || match self.last_full_ms {
                None => true,
                Some(last) => now_ms - last >= self.full_interval_ms,
            };
        if due_full {
            self.reset_pending();
            self.last_full_ms = Some(now_ms);
            self.force_full = false;
            return Some(ReconcileAction::Full);
        }

        // 2) 窗口内脏信号去抖：距首个信号满一个窗口即触发一次。
        if let Some(first) = self.first_dirty_ms {
            if now_ms - first >= self.debounce_window_ms {
                let action = if self.pending_full {
                    ReconcileAction::Full
                } else {
                    ReconcileAction::Dirty(self.pending.iter().cloned().collect())
                };
                self.reset_pending();
                // 去抖触发的定向对账不刷新全量计时；全量仍按自己的节奏兜底。
                return Some(action);
            }
        }
        None
    }

    fn reset_pending(&mut self) {
        self.pending.clear();
        self.pending_full = false;
        self.first_dirty_ms = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dirty(syms: &[&str]) -> AccountDirty {
        AccountDirty {
            instruments: syms.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[test]
    fn first_poll_is_full_startup_sync() {
        let mut d = ReconcileDebouncer::new(500, 60_000);
        assert_eq!(d.poll(1_000), Some(ReconcileAction::Full));
    }

    #[test]
    fn dirty_signals_debounce_into_one_action_after_window() {
        let mut d = ReconcileDebouncer::new(500, 60_000);
        assert_eq!(d.poll(0), Some(ReconcileAction::Full)); // 启动

        d.note_dirty(&dirty(&["BTCUSDT"]), 1_000);
        d.note_dirty(&dirty(&["ETHUSDT", "BTCUSDT"]), 1_200); // 窗口内合并去重
        assert_eq!(d.poll(1_300), None); // 窗口未满
        assert_eq!(
            d.poll(1_500),
            Some(ReconcileAction::Dirty(vec![
                "BTCUSDT".into(),
                "ETHUSDT".into()
            ]))
        );
        // 触发后挂起清空，不再重复。
        assert_eq!(d.poll(1_600), None);
    }

    #[test]
    fn empty_instruments_force_full_after_debounce() {
        let mut d = ReconcileDebouncer::new(500, 60_000);
        d.poll(0);
        d.note_dirty(&dirty(&["BTCUSDT"]), 1_000);
        d.note_dirty(&dirty(&[]), 1_100); // 余额类无标的事件 → 升级全量
        assert_eq!(d.poll(1_500), Some(ReconcileAction::Full));
    }

    #[test]
    fn periodic_full_runs_on_interval_even_without_signals() {
        let mut d = ReconcileDebouncer::new(500, 60_000);
        d.poll(0);
        assert_eq!(d.poll(30_000), None);
        assert_eq!(d.poll(60_000), Some(ReconcileAction::Full));
    }

    #[test]
    fn reconnect_forces_immediate_full() {
        let mut d = ReconcileDebouncer::new(500, 60_000);
        d.poll(1_000);
        d.note_reconnected();
        assert_eq!(d.poll(1_100), Some(ReconcileAction::Full));
    }

    #[test]
    fn full_swallows_pending_dirty_symbols() {
        let mut d = ReconcileDebouncer::new(500, 60_000);
        d.poll(0);
        d.note_dirty(&dirty(&["BTCUSDT"]), 1_000);
        // 定时全量恰好在去抖窗口内到点：全量优先，不再单独发 Dirty。
        assert_eq!(d.poll(60_000), Some(ReconcileAction::Full));
        assert_eq!(d.poll(60_100), None);
    }
}
