//! CTP tick → 1 分钟 K线聚合的纯状态机（仅 CTP 使用，不放通用层）。
//!
//! K线聚合口径：
//! - **按交易日归桶，夜盘归下一交易日**：直接采用 CTP 报文中的 `TradingDay`
//!   （柜台已把 21:00 后的夜盘算入下一交易日），本状态机不重复实现夜盘日历；
//! - **成交量必须差分**：CTP 给的是当日累计量，bar.volume = 本桶末累计量 −
//!   上一桶末累计量；累计额同理；持仓量取桶末值；
//! - **零成交快照不参与 OHLC**：CTP 每 ~500ms 推快照，无成交时累计量不变、最新价
//!   停留在上一笔成交价。只有累计量严格增加的 tick 才算真实成交并进入 OHLC；
//!   整分钟零成交仍出一根 `volume=0` 的延续价 bar（OHLC = 上一根定稿 close，
//!   当日首桶无延续价时取本桶最新快照价），保证出棒连续、下游契约不变；
//! - 跨交易日累计量归零 → 重置差分基线；同桶内累计量回退（乱序/重发）差分钳到 0；
//! - 每个合约一个 [`CandleAggregator`]（由 `bar_agg.rs` 按 symbol 各持一份），
//!   换月即换实例，互不影响。
//!
//! 本类型**不挂钟、不 IO、不 await**：桶归属与定稿完全是输入 tick 的纯函数，
//! 因此无需注入 Clock 即可确定性单测。

use std::collections::BTreeMap;

use rust_decimal::Decimal;

use crate::market::{Kline, KlineExtra, KlineInterval};

/// 一分钟的毫秒数（测试构造 1m 桶/边界用；生产周期来自 `KlineInterval::duration_ms()`）。
#[cfg(test)]
const MINUTE_MS: i64 = 60_000;

/// 聚合器输入：一笔已映射好的 tick（B4 由 FFI 快照转换）。
#[derive(Debug, Clone)]
pub struct CandleTick {
    /// 归属交易日（YYYYMMDD；CTP TradingDay，夜盘已归次日）。
    pub trading_day: u32,
    /// 真实交易所代码（SHFE/DCE/…，取自 FFI ExchangeID；venue 仍是 ctp）。
    pub exchange_id: String,
    /// 交易所时间（ActionDay+UpdateTime+UpdateMillisec 合成的 UTC 毫秒）。
    pub update_ms: i64,
    /// 最新价。
    pub price: Decimal,
    /// 当日累计成交量（手）。
    pub cum_volume: i64,
    /// 当日累计成交额。
    pub cum_turnover: Decimal,
    /// 当前总持仓量。
    pub open_interest: Decimal,
}

/// 把时间戳向下取整到所属周期桶的绝对起点（含）：`floor(ms / interval_ms)`。
///
/// 用 [`i64::div_euclid`] 保证 epoch 之前的负时间戳也向负无穷归桶（边界严格每
/// `interval_ms` 一个），周期边界由此函数按 `interval_ms` 派生，绝不写死 60_000。
fn floor_interval(ms: i64, interval_ms: i64) -> i64 {
    ms.div_euclid(interval_ms) * interval_ms
}

/// 累计量差分（非负钳制：乱序/重发/日切归零都不应产生负成交量）。
fn volume_delta(curr: i64, base: i64) -> i64 {
    (curr - base).max(0)
}

/// 累计额差分（非负）。
fn turnover_delta(curr: Decimal, base: Decimal) -> Decimal {
    (curr - base).max(Decimal::ZERO)
}

/// 一根尚未定稿的桶内中间状态。
struct OpenCandle {
    trading_day: u32,
    /// 真实交易所代码（同合约所有 tick 一致，首笔带定，定稿进 extras）。
    exchange_id: String,
    start_ms: i64,
    /// 桶周期（定稿 end_ms/`Kline.interval` 都由它派生，不写死 1 分钟）。
    interval: KlineInterval,
    /// 本桶内是否已有真实成交（OHLC 是否已初始化）。
    has_trade: bool,
    /// 桶内首个成交价；未成交前为 `None`，避免零成交快照污染 OHLC。
    open: Option<Decimal>,
    high: Option<Decimal>,
    low: Option<Decimal>,
    close: Option<Decimal>,
    /// 本桶最新快照价（成交/快照 tick 都会刷新；零成交桶兜底延续价用）。
    last_price: Decimal,
    /// 本桶起点的累计量基线（= 上一桶最后一笔 tick 的累计量；跨日重置为首笔值）。
    base_volume: i64,
    /// 本桶起点的累计额基线。
    base_turnover: Decimal,
    /// 上一笔 tick 的累计量（翻转时作为下一桶基线）。
    last_cum_volume: i64,
    /// 上一笔 tick 的累计额。
    last_cum_turnover: Decimal,
    /// 桶内成交量（累计量差分，非负）。
    volume: i64,
    /// 桶内成交额（差分，非负；定稿即公共 quote_volume）。
    quote_volume: Decimal,
    /// 桶末持仓量（每笔覆盖；定稿进 extras.open_interest）。
    open_interest: Decimal,
}

impl OpenCandle {
    /// 用新桶的第一笔 tick 建立中间状态；首笔自身相对基线的差分计入桶量。
    /// `is_trade` 为 false 时（零成交快照）不初始化 OHLC，仅记录最新价。
    fn first(
        t: &CandleTick,
        start_ms: i64,
        interval: KlineInterval,
        base_volume: i64,
        base_turnover: Decimal,
        is_trade: bool,
    ) -> Self {
        // 只有成交 tick 才初始化 OHLC；纯快照桶等待首个成交或定稿时取延续价。
        let (open, high, low, close) = if is_trade {
            (Some(t.price), Some(t.price), Some(t.price), Some(t.price))
        } else {
            (None, None, None, None)
        };
        Self {
            trading_day: t.trading_day,
            exchange_id: t.exchange_id.clone(),
            start_ms,
            interval,
            has_trade: is_trade,
            open,
            high,
            low,
            close,
            last_price: t.price,
            base_volume,
            base_turnover,
            last_cum_volume: t.cum_volume,
            last_cum_turnover: t.cum_turnover,
            volume: volume_delta(t.cum_volume, base_volume),
            quote_volume: turnover_delta(t.cum_turnover, base_turnover),
            open_interest: t.open_interest,
        }
    }

    /// 同桶内追加一笔：仅真实成交更新 OHLC；任何 tick 都刷新持仓/量额差分与最新价。
    fn fold(&mut self, t: &CandleTick, is_trade: bool) {
        self.last_price = t.price;
        if is_trade {
            if self.has_trade {
                // 已有成交：扩展 high/low、close 前移，open 保持不变。
                self.high = Some(self.high.unwrap_or(t.price).max(t.price));
                self.low = Some(self.low.unwrap_or(t.price).min(t.price));
                self.close = Some(t.price);
            } else {
                // 本桶首个成交：OHLC 四值一次性初始化为成交价。
                self.open = Some(t.price);
                self.high = Some(t.price);
                self.low = Some(t.price);
                self.close = Some(t.price);
                self.has_trade = true;
            }
        }
        self.volume = volume_delta(t.cum_volume, self.base_volume);
        self.quote_volume = turnover_delta(t.cum_turnover, self.base_turnover);
        self.open_interest = t.open_interest;
        self.last_cum_volume = t.cum_volume;
        self.last_cum_turnover = t.cum_turnover;
    }

    /// 定稿为不可变 [`Kline`]（结束时间为半开区间 [start, start+interval)）。
    ///
    /// 零成交桶按方案 B 出延续价：`prev_close`（同一交易日上一根定稿 close）优先，
    /// 当日首桶无延续价时退化为本桶最新快照价；此时量/额已天然为 0。
    ///
    /// CTP 专有指标经 extras 平铺：`exchange_id`（Str，真实交易所）与
    /// `open_interest`（Num，周期末持仓量）；成交额并入公共 `quote_volume`。
    fn finalize(&self, venue: &str, symbol: &str, prev_close: Option<Decimal>) -> Kline {
        let mut extras = BTreeMap::new();
        extras.insert(
            "exchange_id".to_string(),
            KlineExtra::Str(self.exchange_id.clone()),
        );
        extras.insert(
            "open_interest".to_string(),
            KlineExtra::Num(self.open_interest),
        );
        // 有成交：取成交 tick 构成的 OHLC（理论上均为 Some，兜底最新价）；
        // 零成交：四值统一为延续价，保证出棒连续。
        let (open, high, low, close) = if self.has_trade {
            (
                self.open.unwrap_or(self.last_price),
                self.high.unwrap_or(self.last_price),
                self.low.unwrap_or(self.last_price),
                self.close.unwrap_or(self.last_price),
            )
        } else {
            let carry = prev_close.unwrap_or(self.last_price);
            (carry, carry, carry, carry)
        };
        Kline {
            venue: venue.to_string(),
            symbol: symbol.to_string(),
            interval: self.interval,
            trading_day: self.trading_day,
            start_ms: self.start_ms,
            end_ms: self.start_ms + self.interval.duration_ms(),
            open,
            high,
            low,
            close,
            volume: self.volume,
            quote_volume: self.quote_volume,
            extras,
        }
    }
}

/// 单合约周期聚合器（本期仅 1 分钟；周期由 [`KlineInterval`] 参数化）。
pub struct CandleAggregator {
    /// 交易所标识（写入 Kline.venue）。
    venue: String,
    /// 合约代码（如实写入 Kline.symbol，如 cu2512）。
    symbol: String,
    /// 桶周期（归桶边界/end_ms/`Kline.interval` 都由它派生；本期 M1，为 M30 留口）。
    interval: KlineInterval,
    open: Option<OpenCandle>,
    /// 最近一次已定稿桶的（交易日, 绝对起点）；用于丢弃定稿后到达的迟到 tick。
    last_finalized: Option<(u32, i64)>,
    /// 最近一笔已并入 tick 的差分基线 `(交易日, 当日累计量, 当日累计额)`。**独立于在途桶
    /// 存活**：墙钟 take 走桶后仍保留，使同日下一桶（含跨空档首笔）正确差分、量守恒；
    /// 交易日变化时按本笔重置。`None` = 尚无 tick。该元组同时是「成交判定」基线：
    /// 当日累计量严格增加才算真实成交。
    baseline: Option<(u32, i64, Decimal)>,
    /// 最近一根已定稿 bar 的 `(交易日, 收盘价)`：零成交桶的延续价来源。
    /// 跨交易日后不再沿用（定稿时按交易日过滤）。
    last_close: Option<(u32, Decimal)>,
}

impl CandleAggregator {
    pub fn new(
        venue: impl Into<String>,
        symbol: impl Into<String>,
        interval: KlineInterval,
    ) -> Self {
        Self {
            venue: venue.into(),
            symbol: symbol.into(),
            interval,
            open: None,
            last_finalized: None,
            baseline: None,
            last_close: None,
        }
    }

    /// 该 tick 是否为「已定稿桶的迟到 tick」：同交易日且桶起点不晚于最近定稿起点。
    /// 按 (交易日, 起点) 双键判定，跨交易日即便 ms 数值更小也不算迟到（见跨日测试）。
    fn is_late(&self, trading_day: u32, start: i64) -> bool {
        matches!(self.last_finalized, Some((d, s)) if d == trading_day && start <= s)
    }

    /// 记录某桶已定稿（事件时间墙钟 / poll 拍墙钟 / flush 收口都经此，防重复出棒）。
    fn mark_finalized(&mut self, trading_day: u32, start: i64) {
        self.last_finalized = Some((trading_day, start));
    }

    /// 本笔是否为真实成交：以「上一笔 tick 的当日累计量」差分判定。
    ///
    /// 无成交快照的累计量不变（价格停留在上一笔成交价），故只有累计量严格增加才是
    /// 成交并进入 OHLC。跨交易日累计量归零/重置，基线按日失效：当日首笔观测到非零
    /// 累计量即视为成交（否则新交易日首根 bar 会丢失 OHLC）。
    fn is_trade(&self, t: &CandleTick) -> bool {
        match self.baseline {
            Some((day, vol, _)) if day == t.trading_day => t.cum_volume > vol,
            _ => t.cum_volume > 0,
        }
    }

    /// 丢弃 tick 的统一 debug 日志（迟到 / 乱序两路共用），不改动任何状态。
    fn trace_drop(&self, t: &CandleTick, start: i64, reason: &str) {
        tracing::debug!(
            venue = %self.venue,
            symbol = %self.symbol,
            trading_day = t.trading_day,
            tick_ms = t.update_ms,
            bucket_start = start,
            "丢弃 CTP tick：{reason}"
        );
    }

    /// 喂入一笔 tick。**定稿唯一由墙钟判定**（grace=0，到 `start+interval` 边界即定稿）：
    /// 1. 先以本笔事件时间 [`Self::finalize_if_due`] 收口到边界的旧桶；若在途桶属上一交易日
    ///    （trading_day 独立于分钟墙钟、跨日 ms 不可比）再按日切收口——两处都是 take，最多出一根；
    /// 2. 迟到 tick（所属 `(交易日,起点)` 已定稿）丢弃并 `debug!`，不重开/不并入；
    /// 3. 同桶 fold，否则开新桶。
    ///
    /// 差分基线独立存在聚合器外层 `baseline`（见 [`Self::open_candle`]），故墙钟 take 走旧桶后
    /// 跨空档首笔的增量仍正确并入、量守恒。返回本笔触发收口的定稿 [`Kline`]（否则 `None`）。
    pub fn on_tick(&mut self, t: &CandleTick) -> Option<Kline> {
        let start = floor_interval(t.update_ms, self.interval.duration_ms());
        // 1) 事件时间墙钟收口；在途桶属上一交易日则再按日切收口。
        let mut finished = self.finalize_if_due(t.update_ms);
        if matches!(&self.open, Some(c) if c.trading_day != t.trading_day) {
            finished = self.take_and_finalize().or(finished);
        }
        // 2) 迟到防护：收口后本笔仍落在已定稿桶 → 丢弃（刚收口的更早桶定稿照常上抛）。
        if self.is_late(t.trading_day, start) {
            self.trace_drop(t, start, "迟到：所属桶已墙钟定稿");
            return finished;
        }
        // 2b) 乱序防护：同交易日、tick 桶起点早于**当前在途桶**（介于最近定稿桶与在途桶之间的
        // 空窗乱序/重连快照）。它无桶可并，丢弃并保留在途桶与基线，绝不用旧桶覆盖在途桶。
        if matches!(&self.open, Some(c) if c.trading_day == t.trading_day && start < c.start_ms) {
            self.trace_drop(t, start, "乱序：早于当前在途桶");
            return finished;
        }
        // 3) 同桶 fold；否则（首笔/墙钟已收口/跨日，在途必为空）开新桶。
        //    成交判定先于折叠：以本笔之前的累计量为基线（跨日自动失效）。
        let is_trade = self.is_trade(t);
        match self.open.take() {
            Some(mut c) if c.trading_day == t.trading_day && c.start_ms == start => {
                c.fold(t, is_trade);
                self.open = Some(c);
            }
            _ => self.open = Some(self.open_candle(t, start, is_trade)),
        }
        // 推进外层差分基线（独立于在途桶存活，墙钟 take 不清它）。
        self.baseline = Some((t.trading_day, t.cum_volume, t.cum_turnover));
        finished
    }

    /// 开新桶并定差分基线：同交易日接上一笔累计量（该笔可能属于已被墙钟 take 的旧桶，
    /// 故基线存聚合器外层而非桶内）；跨交易日或当日首笔则累计归零、基线取本笔。
    fn open_candle(&self, t: &CandleTick, start: i64, is_trade: bool) -> OpenCandle {
        let (base_volume, base_turnover) = match self.baseline {
            Some((day, vol, ref turn)) if day == t.trading_day => (vol, *turn),
            _ => (t.cum_volume, t.cum_turnover),
        };
        OpenCandle::first(
            t,
            start,
            self.interval,
            base_volume,
            base_turnover,
            is_trade,
        )
    }

    /// `take` 出在途桶，登记定稿点并定稿为 [`Kline`]；无桶返回 `None`。
    /// 只动 `open`/`last_finalized`/`last_close`，**不清外层 `baseline`**，
    /// 跨空档下一桶据此续算差分。
    fn take_and_finalize(&mut self) -> Option<Kline> {
        self.open.take().map(|c| {
            self.mark_finalized(c.trading_day, c.start_ms);
            // 延续价仅在同一交易日内有效（跨日收盘不能作为次日零成交桶的 OHLC）。
            let prev_close = match self.last_close {
                Some((day, close)) if day == c.trading_day => Some(close),
                _ => None,
            };
            let bar = c.finalize(&self.venue, &self.symbol, prev_close);
            self.last_close = Some((c.trading_day, bar.close));
            bar
        })
    }

    /// 墙钟到边界定稿（grace=0）：存在在途桶且 `start + interval <= now_ms` 即收口。
    /// 两个调用点共用此唯一判定——md 线程每拍的空闲 sweep（`now_ms`=SystemTime）与
    /// `on_tick` 的事件时间收口（`now_ms`=tick 时间）。未到点或无桶返回 `None`；
    /// 同一桶最多定稿一次（take 语义）。
    pub fn finalize_if_due(&mut self, now_ms: i64) -> Option<Kline> {
        // 无在途桶直接 None；借用在语句末结束（i64 Copy），随后可安全 take。
        let due_at = self.open.as_ref()?.start_ms + self.interval.duration_ms();
        if now_ms < due_at {
            return None;
        }
        self.take_and_finalize()
    }

    /// 强制定稿当前未闭桶（日切/停机/测试用）；无桶返回 `None`，并清空当前桶。
    pub fn flush(&mut self) -> Option<Kline> {
        self.take_and_finalize()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dec(v: i64) -> Decimal {
        Decimal::from(v)
    }

    fn tick(day: u32, ms: i64, price: i64, vol: i64, turnover: i64, oi: i64) -> CandleTick {
        tick_ex("SHFE", day, ms, price, vol, turnover, oi)
    }

    /// 可指定真实交易所代码（SHFE/DCE/…）的 tick 构造。
    fn tick_ex(
        exchange_id: &str,
        day: u32,
        ms: i64,
        price: i64,
        vol: i64,
        turnover: i64,
        oi: i64,
    ) -> CandleTick {
        CandleTick {
            trading_day: day,
            exchange_id: exchange_id.into(),
            update_ms: ms,
            price: Decimal::from(price),
            cum_volume: vol,
            cum_turnover: Decimal::from(turnover),
            open_interest: Decimal::from(oi),
        }
    }

    /// 读定稿 bar extras 里的周期末持仓量（Num/Int 统一按数值口径）。
    fn bar_oi(b: &Kline) -> Decimal {
        b.extras
            .get("open_interest")
            .and_then(KlineExtra::as_decimal)
            .unwrap()
    }

    // 真实交易所代码随 tick 透传进定稿 bar extras（SHFE 上期 / DCE 大商两类合约）。
    #[test]
    fn finalized_bar_carries_native_exchange_id_extra() {
        use crate::market::KlineExtra;
        // SHFE 铜：定稿 bar.extras.exchange_id = "SHFE"。
        let mut shfe = CandleAggregator::new("ctp", "cu2512", KlineInterval::M1);
        shfe.on_tick(&tick_ex("SHFE", 20261023, 600_000, 100, 10, 1000, 5));
        let bar = shfe.finalize_if_due(660_000).expect("SHFE 定稿");
        assert_eq!(
            bar.extras.get("exchange_id"),
            Some(&KlineExtra::Str("SHFE".into()))
        );

        // DCE 豆粕：不同 exchange_id 也如实进 extras（venue 仍是 ctp）。
        let mut dce = CandleAggregator::new("ctp", "m2512", KlineInterval::M1);
        dce.on_tick(&tick_ex("DCE", 20261023, 600_000, 3000, 10, 1000, 5));
        let bar = dce.finalize_if_due(660_000).expect("DCE 定稿");
        assert_eq!(bar.venue, "ctp");
        assert_eq!(
            bar.extras.get("exchange_id"),
            Some(&KlineExtra::Str("DCE".into()))
        );
    }

    // 同一分钟两笔：不开新 bar，OHLC 在桶内累积。
    #[test]
    fn ticks_within_same_minute_update_ohlc_no_finalize() {
        let mut agg = CandleAggregator::new("ctp", "cu2512", KlineInterval::M1);
        // 10:00:00.000 与 10:00:30.000（同桶起点 600_000）。
        assert!(agg
            .on_tick(&tick(20261023, 600_000, 100, 10, 1000, 5))
            .is_none());
        assert!(agg
            .on_tick(&tick(20261023, 630_000, 110, 15, 1600, 7))
            .is_none());
        // flush 得到当前桶：高低收与差分正确。
        let bar = agg.flush().expect("当前桶");
        assert_eq!(bar.venue, "ctp");
        assert_eq!(bar.symbol, "cu2512");
        assert_eq!(bar.trading_day, 20261023);
        assert_eq!(bar.start_ms, 600_000);
        assert_eq!(bar.end_ms, 660_000);
        assert_eq!(bar.open, dec(100));
        assert_eq!(bar.high, dec(110));
        assert_eq!(bar.low, dec(100));
        assert_eq!(bar.close, dec(110));
        // 量 = 桶末累计 15 − 首笔基线 10 = 5；额 = 1600−1000 = 600；持仓取桶末 7。
        assert_eq!(bar.volume, 5);
        assert_eq!(bar.quote_volume, dec(600));
        assert_eq!(bar_oi(&bar), dec(7));
    }

    // 跨分钟：第二分钟首笔触发上一分钟定稿，量为分钟末累计差分。
    #[test]
    fn minute_flip_finalizes_previous_with_volume_diff() {
        let mut agg = CandleAggregator::new("ctp", "rb2601", KlineInterval::M1);
        // 第一分钟 10:00 两笔：累计 10→15。
        agg.on_tick(&tick(20261023, 600_000, 3800, 10, 100, 1));
        agg.on_tick(&tick(20261023, 630_000, 3801, 15, 160, 2));
        // 第二分钟 10:01 首笔，累计 15→17：定稿上一桶。
        let finished = agg
            .on_tick(&tick(20261023, 660_000, 3802, 17, 200, 3))
            .expect("上一分钟定稿");
        assert_eq!(finished.start_ms, 600_000);
        assert_eq!(finished.end_ms, 660_000);
        // 上一分钟量 = 15 − 10 = 5（不是累计 15）。
        assert_eq!(finished.volume, 5);
        assert_eq!(finished.quote_volume, dec(60));
        assert_eq!(finished.close, dec(3801));
        assert_eq!(bar_oi(&finished), dec(2));
        // 当前新桶再 flush：基线=上桶末累计 15，量 = 17−15 = 2。
        let cur = agg.flush().expect("新桶");
        assert_eq!(cur.start_ms, 660_000);
        assert_eq!(cur.volume, 2);
        assert_eq!(cur.quote_volume, dec(40));
    }

    // 夜盘归次日：TradingDay 由柜台给次日，21:00 与 21:01 同交易日，分钟桶正常翻转。
    #[test]
    fn night_session_uses_counter_trading_day() {
        let mut agg = CandleAggregator::new("ctp", "ag2512", KlineInterval::M1);
        // 周一 21:00:00 / 21:00:30 同分钟，累计 100→108，TradingDay 已为周二 20261027。
        agg.on_tick(&tick(20261027, 75_600_000, 8000, 100, 1000, 10));
        agg.on_tick(&tick(20261027, 75_630_000, 8004, 108, 1090, 11));
        // 21:01 首笔翻转，定稿的夜盘第一根 bar 归次日交易日。
        let night = agg
            .on_tick(&tick(20261027, 75_660_000, 8005, 112, 1150, 12))
            .expect("夜盘上一分钟定稿");
        assert_eq!(night.trading_day, 20261027);
        assert_eq!(night.start_ms, 75_600_000);
        assert_eq!(night.volume, 8);
        assert_eq!(night.quote_volume, dec(90));
        assert_eq!(bar_oi(&night), dec(11));
    }

    // 跨交易日：累计量归零，新交易日首桶基线重置，不出现负量，定稿旧桶不被污染。
    #[test]
    fn trading_day_change_resets_cumulative_baseline() {
        let mut agg = CandleAggregator::new("ctp", "cu2512", KlineInterval::M1);
        // 前一日 14:59 累计到 50000。
        agg.on_tick(&tick(20261023, 53_940_000, 70_000, 50_000, 9_999, 100));
        // 次日 09:00 累计重新从小计数（CTP 日初归零/集合竞价），trading_day 变化。
        let finished = agg
            .on_tick(&tick(20261024, 32_400_000, 70_100, 2, 3, 101))
            .expect("前一日桶定稿");
        assert_eq!(finished.trading_day, 20261023);
        assert_eq!(finished.volume, 0); // 前一桶只有首笔，量 0
                                        // 新桶再来一笔累计 5 → 基线已重置为 2，量 3，绝不为负。
        agg.on_tick(&tick(20261024, 32_430_000, 70_110, 5, 9, 102));
        let cur = agg.flush().expect("新交易日桶");
        assert_eq!(cur.trading_day, 20261024);
        assert_eq!(cur.volume, 3);
        assert_eq!(cur.quote_volume, dec(6));
    }

    // 同桶累计量回退（乱序/重发）：差分钳到 0，不产生负成交量。
    #[test]
    fn same_bucket_cumulative_regression_clamps_to_zero() {
        let mut agg = CandleAggregator::new("ctp", "rb2601", KlineInterval::M1);
        agg.on_tick(&tick(20261023, 600_000, 3800, 100, 1000, 1));
        agg.on_tick(&tick(20261023, 620_000, 3801, 90, 900, 1));
        let bar = agg.flush().unwrap();
        assert_eq!(bar.volume, 0);
        assert_eq!(bar.quote_volume, dec(0));
    }

    // 最低价在下跌中更新（累计量递增 = 真实成交，OHLC 才会被采纳）。
    #[test]
    fn high_low_track_extremes() {
        let mut agg = CandleAggregator::new("ctp", "cu2512", KlineInterval::M1);
        agg.on_tick(&tick(20261023, 600_000, 100, 10, 0, 0));
        agg.on_tick(&tick(20261023, 605_000, 99, 11, 0, 0));
        agg.on_tick(&tick(20261023, 610_000, 101, 12, 0, 0));
        let bar = agg.flush().unwrap();
        assert_eq!(bar.low, dec(99));
        assert_eq!(bar.high, dec(101));
        assert_eq!(bar.interval, KlineInterval::M1);
    }

    #[test]
    fn flush_empty_is_none() {
        let mut agg = CandleAggregator::new("ctp", "x", KlineInterval::M1);
        assert!(agg.flush().is_none());
    }

    #[test]
    fn floor_minute_buckets_correctly() {
        assert_eq!(floor_interval(0, MINUTE_MS), 0);
        assert_eq!(floor_interval(59_999, MINUTE_MS), 0);
        assert_eq!(floor_interval(60_000, MINUTE_MS), 60_000);
        assert_eq!(floor_interval(61_234, MINUTE_MS), 60_000);
    }

    // 绝对边界必须按 interval 参数算、不写死 60_000：用 30m（1_800_000ms）验证
    // 对齐到每半小时间的绝对 epoch 边界（:00/:30），为未来 KlineInterval::M30 留口。
    #[test]
    fn floor_interval_aligns_arbitrary_interval_to_absolute_epoch_edge() {
        const M30_MS: i64 = 30 * 60_000;
        // 00:29:59.999 → 00:00；00:30:00.000 → 00:30；00:45:10.234 → 00:30。
        assert_eq!(floor_interval(30 * 60_000 - 1, M30_MS), 0);
        assert_eq!(floor_interval(M30_MS, M30_MS), M30_MS);
        assert_eq!(floor_interval(M30_MS + 910_234, M30_MS), M30_MS);
        // 跨小时：01:15:00 → 01:00（= 2 * M30）。
        assert_eq!(floor_interval(75 * 60_000, M30_MS), 60 * 60_000);
        // 边界序列严格每 interval 一个：边界后任意偏移都落回本边界。
        for k in 0..4 {
            let edge = k * M30_MS;
            assert_eq!(floor_interval(edge + 123_456, M30_MS), edge);
        }
    }

    // 特征钉死（方案①·事件时间归属）：稀疏 tick 跨分钟缺口时，缺口期空分钟不出 bar，
    // 增量随「周期结束后第一根 tick」归入其所在桶。样例：22/23/27/29，cum 30/60/80/90/100。
    #[test]
    fn sparse_gap_ticks_attribute_volume_by_event_time() {
        const DAY: u32 = 20261027;
        // 分钟:秒 → 当日毫秒（与 update_ms 同坐标系）。
        let m = |mm: i64, sec: i64| mm * 60_000 + sec * 1_000;
        let mut agg = CandleAggregator::new("ctp", "cu2701", KlineInterval::M1);
        let mut finished = Vec::new();

        // t1 22:59 cum=30（当日首观测点，无基线）。
        assert!(agg.on_tick(&tick(DAY, m(22, 59), 1000, 30, 0, 1)).is_none());
        // t2 23:10 cum=60 → 定稿 22 桶。
        if let Some(b) = agg.on_tick(&tick(DAY, m(23, 10), 1010, 60, 0, 2)) {
            finished.push(b);
        }
        // t3 27:05 cum=80 → 定稿 23 桶（跨过 24/25/26，空分钟不出 bar）。
        if let Some(b) = agg.on_tick(&tick(DAY, m(27, 5), 1030, 80, 0, 3)) {
            finished.push(b);
        }
        // t4 27:58 cum=90 同桶 fold，不产 bar。
        assert!(agg.on_tick(&tick(DAY, m(27, 58), 1025, 90, 0, 4)).is_none());
        // t5 29:03 cum=100 → 定稿 27 桶（跨过 28）。
        if let Some(b) = agg.on_tick(&tick(DAY, m(29, 3), 1050, 100, 0, 5)) {
            finished.push(b);
        }

        // on_tick 仅定稿 3 根：22 / 23 / 27。
        assert_eq!(finished.len(), 3);
        let starts: Vec<i64> = finished.iter().map(|b| b.start_ms).collect();
        assert_eq!(starts, vec![m(22, 0), m(23, 0), m(27, 0)]);

        // 22 桶：单笔 1000，当日首观测点差分记 0，持仓取桶末 1。
        let b22 = &finished[0];
        assert_eq!(b22.open, dec(1000));
        assert_eq!(b22.high, dec(1000));
        assert_eq!(b22.low, dec(1000));
        assert_eq!(b22.close, dec(1000));
        assert_eq!(b22.volume, 0);
        assert_eq!(bar_oi(b22), dec(1));

        // 23 桶：单笔 1010，量 = 60−30 = 30，持仓 2。
        let b23 = &finished[1];
        assert_eq!(b23.open, dec(1010));
        assert_eq!(b23.volume, 30);
        assert_eq!(bar_oi(b23), dec(2));

        // 27 桶：O=1030（t3），H=1030，L=C=1025（t4），量 = 90−60 = 30，持仓取桶末 4。
        let b27 = &finished[2];
        assert_eq!(b27.open, dec(1030));
        assert_eq!(b27.high, dec(1030));
        assert_eq!(b27.low, dec(1025));
        assert_eq!(b27.close, dec(1025));
        assert_eq!(b27.volume, 30);
        assert_eq!(bar_oi(b27), dec(4));

        // 缺口空分钟 24/25/26/28 绝不出 bar。
        assert!(finished
            .iter()
            .all(|b| ![m(24, 0), m(25, 0), m(26, 0), m(28, 0)].contains(&b.start_ms)));

        // flush 收口最后活跃桶 29：单笔 1050，量 = 100−90 = 10，持仓 5。
        let b29 = agg.flush().expect("最后活跃桶");
        assert_eq!(b29.start_ms, m(29, 0));
        assert_eq!(b29.open, dec(1050));
        assert_eq!(b29.volume, 10);
        assert_eq!(bar_oi(&b29), dec(5));

        // 总量守恒：0+30+30+10 = 70 = 末累计 100 − 首观测点基线 30。
        let total: i64 = finished.iter().map(|b| b.volume).sum::<i64>() + b29.volume;
        assert_eq!(total, 70);
        // flush 后再 flush 无桶。
        assert!(agg.flush().is_none());
    }

    // 成交额与成交量同口径：按累计额差分（非负），跨分钟缺口同样归入恢复桶。
    #[test]
    fn sparse_gap_turnover_uses_cumulative_delta() {
        const DAY: u32 = 20261027;
        let m = |mm: i64, sec: i64| mm * 60_000 + sec * 1_000;
        let mut agg = CandleAggregator::new("ctp", "cu2701", KlineInterval::M1);
        agg.on_tick(&tick(DAY, m(23, 10), 1010, 60, 600_000, 2));
        // 跨过 24/25/26，27:05 累计额 800_000：定稿 23 桶，额 = 600_000 − 首笔基线 0。
        let b23 = agg
            .on_tick(&tick(DAY, m(27, 5), 1030, 80, 800_000, 3))
            .expect("23 桶定稿");
        // 23 桶首笔基线为其自身累计额，故差分 0。
        assert_eq!(b23.quote_volume, dec(0));
        // 27 桶再来一笔累计额 900_000，flush 收口：额 = 900_000 − 600_000 = 300_000。
        agg.on_tick(&tick(DAY, m(27, 58), 1025, 90, 900_000, 4));
        let b27 = agg.flush().expect("27 桶");
        assert_eq!(b27.quote_volume, dec(300_000));
    }

    // 跨桶首笔累计量回退（乱序/重发导致比上一桶末累计还小）：新桶差分钳 0，不为负。
    #[test]
    fn cross_bucket_cumulative_regression_clamps_to_zero() {
        const DAY: u32 = 20261027;
        let m = |mm: i64, sec: i64| mm * 60_000 + sec * 1_000;
        let mut agg = CandleAggregator::new("ctp", "rb2601", KlineInterval::M1);
        agg.on_tick(&tick(DAY, m(10, 0), 3800, 100, 1_000, 1));
        // 下一分钟首笔累计量异常回退到 90（< 上一桶末 100）：先定稿旧桶。
        let prev = agg
            .on_tick(&tick(DAY, m(11, 0), 3799, 90, 900, 1))
            .expect("上一桶定稿");
        assert_eq!(prev.volume, 0); // 上一桶仅首笔，差分 0
                                    // 新桶基线=100，本笔 90 回退 → 钳 0，绝不出现负成交量。
        let cur = agg.flush().expect("新桶");
        assert_eq!(cur.volume, 0);
        assert_eq!(cur.quote_volume, dec(0));
    }

    // ===== 墙钟到点定稿（不再等下一周期首笔 tick）=====

    // 到 start+interval 墙钟边界即定稿（grace=0）：finalize_if_due 出上一桶，OHLC/量差分正确。
    #[test]
    fn wallclock_finalize_emits_bucket_at_boundary() {
        let mut agg = CandleAggregator::new("ctp", "cu2701", KlineInterval::M1);
        agg.on_tick(&tick(20261027, 600_000, 100, 10, 1000, 1));
        agg.on_tick(&tick(20261027, 630_000, 110, 15, 1600, 2));
        // 收盘边界 660_000 之前不定稿。
        assert!(agg.finalize_if_due(659_999).is_none());
        // 到边界即定稿：无需等到 10:01 的 tick。
        let bar = agg
            .finalize_if_due(660_000)
            .expect("墙钟到边界应定稿 10:00 桶");
        assert_eq!(bar.start_ms, 600_000);
        assert_eq!(bar.end_ms, 660_000);
        assert_eq!(bar.interval, KlineInterval::M1);
        assert_eq!(bar.open, dec(100));
        assert_eq!(bar.high, dec(110));
        assert_eq!(bar.low, dec(100));
        assert_eq!(bar.close, dec(110));
        assert_eq!(bar.volume, 5);
        assert_eq!(bar.quote_volume, dec(600));
        assert_eq!(bar_oi(&bar), dec(2));
    }

    // 空窗（从无 tick 的合约/分钟）无聚合器桶 → 不产棒；take 语义下同拍/再拍不重复出。
    #[test]
    fn wallclock_empty_and_repeat_sweep_emit_nothing() {
        let mut agg = CandleAggregator::new("ctp", "idle", KlineInterval::M1);
        assert!(agg.finalize_if_due(10_000_000).is_none());
        agg.on_tick(&tick(20261027, 600_000, 100, 10, 0, 1));
        assert!(agg.finalize_if_due(660_000).is_some());
        assert!(agg.finalize_if_due(660_000).is_none());
        assert!(agg.finalize_if_due(999_999_999).is_none());
    }

    // 墙钟定稿后到达的「本桶迟到 tick」必须丢弃，绝不重开旧桶/重复出棒/并入量。
    #[test]
    fn wallclock_late_tick_after_sweep_is_dropped_without_reopen() {
        let mut agg = CandleAggregator::new("ctp", "cu2701", KlineInterval::M1);
        agg.on_tick(&tick(20261027, 600_000, 100, 10, 0, 1));
        agg.on_tick(&tick(20261027, 630_000, 101, 12, 0, 1));
        assert!(agg.finalize_if_due(660_000).is_some());
        // 时间戳仍属 10:00 桶（floor == 已定稿起点）：丢弃。
        assert!(agg
            .on_tick(&tick(20261027, 600_500, 99, 99, 0, 9))
            .is_none());
        assert!(agg.finalize_if_due(999_999_999).is_none());
        assert!(agg.flush().is_none());
    }

    // 定稿后下一周期的正常 tick 照常开新桶（迟到防护不得越界误杀）；基线在聚合器外层
    // 持久化，跨空档（sweep 已 take 旧桶）的累计增量并入新桶，总量守恒不丢。
    #[test]
    fn wallclock_then_gap_tick_attributes_increment_to_new_bucket() {
        let mut agg = CandleAggregator::new("ctp", "cu2701", KlineInterval::M1);
        agg.on_tick(&tick(20261027, 600_000, 100, 10, 0, 1));
        assert!(agg.finalize_if_due(660_000).is_some());
        // 10:01:30（floor 660_000 > 定稿 600_000）：开新桶，不产棒。
        assert!(agg
            .on_tick(&tick(20261027, 690_000, 102, 20, 0, 2))
            .is_none());
        let cur = agg.flush().expect("新周期桶");
        assert_eq!(cur.start_ms, 660_000);
        assert_eq!(cur.end_ms, 720_000);
        // 基线持久化为上一定稿桶末累计 10：空档+本桶首笔增量 20−10 = 10，不丢、不回填旧桶。
        assert_eq!(cur.volume, 10);
    }

    // 唯一墙钟定稿：新周期首笔 tick 自带事件时间已到边界 → on_tick 内嵌墙钟先收旧桶，
    // 再开新桶；与 poll 拍 sweep 对同一在途桶最多各出一次、绝不重复。
    #[test]
    fn tick_driven_and_poll_driven_wallclock_finalize_at_most_once() {
        let mut agg = CandleAggregator::new("ctp", "rb2601", KlineInterval::M1);
        agg.on_tick(&tick(20261027, 600_000, 3800, 10, 0, 1));
        // 10:01 首笔（事件时间 660_000 恰好到边界）：内嵌墙钟定稿 10:00（首桶量 0）。
        let finished = agg
            .on_tick(&tick(20261027, 660_000, 3801, 12, 0, 2))
            .expect("tick 驱动墙钟定稿 10:00");
        assert_eq!(finished.start_ms, 600_000);
        // poll 拍再 sweep：在途已是 10:01 桶（未到 720_000），10:00 不会再出。
        assert!(agg.finalize_if_due(660_000).is_none());
        // 10:01 到边界 sweep 只出 10:01：基线接上一桶末累计 10，量 = 12−10 = 2。
        let swept = agg.finalize_if_due(720_000).expect("墙钟定稿 10:01");
        assert_eq!(swept.start_ms, 660_000);
        assert_eq!(swept.volume, 2);
        assert!(agg.finalize_if_due(999_999_999).is_none());
    }

    // 真机回归（jm2701 2026-10-28）：sweep 定稿 → 跨 31 分钟空档 → 新桶首笔，
    // 空档成交（34→40，6 手）必须并入新桶而非丢失。
    #[test]
    fn wallclock_long_gap_conserves_cumulative_volume() {
        let mut agg = CandleAggregator::new("ctp", "jm2701", KlineInterval::M1);
        let m = |mm: i64, sec: i64| mm * 60_000 + sec * 1_000;
        // 13:52:51 累计 34（13:52 桶，分钟按当日 0 点起算：13:52=832m）。
        agg.on_tick(&tick(20261028, m(832, 51), 1378, 34, 0, 1883));
        // 13:53 边界墙钟定稿 13:52 桶。
        let b52 = agg.finalize_if_due(m(833, 0)).expect("13:52 定稿");
        assert_eq!(b52.start_ms, m(832, 0));
        // 31 分钟后 14:24:24 首笔，累计跳到 40：开 14:24 桶，不产棒。
        assert!(agg
            .on_tick(&tick(20261028, m(864, 24), 1390, 40, 0, 1884))
            .is_none());
        // 14:25 边界定稿 14:24 桶：量 = 40−34 = 6（空档成交不丢）。
        let b24 = agg.finalize_if_due(m(865, 0)).expect("14:24 定稿");
        assert_eq!(b24.start_ms, m(864, 0));
        assert_eq!(b24.volume, 6);
    }

    // 迟到防护按 (交易日, 起点) 判定：跨交易日即便 ms 数值更小也必须放行并重置基线。
    #[test]
    fn wallclock_sweep_then_new_trading_day_tick_is_accepted() {
        let mut agg = CandleAggregator::new("ctp", "cu2512", KlineInterval::M1);
        // 前一交易日 14:59 桶墙钟定稿（grace=0：到 start+interval 即收口）。
        agg.on_tick(&tick(20261023, 53_940_000, 70_000, 50_000, 0, 100));
        let d1 = agg
            .finalize_if_due(53_940_000 + 60_000)
            .expect("前日桶定稿");
        assert_eq!(d1.trading_day, 20261023);
        // 次日 09:00（日相对 ms 32_400_000 < 前日 53_940_000；trading_day 不同）：放行。
        assert!(agg
            .on_tick(&tick(20261024, 32_400_000, 70_100, 2, 0, 101))
            .is_none());
        let d2 = agg.flush().expect("新交易日桶");
        assert_eq!(d2.trading_day, 20261024);
        assert_eq!(d2.start_ms, 32_400_000);
        assert_eq!(d2.volume, 0);
    }

    // 乱序 tick（floor 落在「最近定稿桶之后、当前在途桶之前」的空窗）必须丢弃，
    // 绝不能用更早的桶覆盖当前在途桶（否则在途桶连同其累计量被静默顶丢）。
    #[test]
    fn out_of_order_gap_tick_is_dropped_without_clobbering_open_bucket() {
        let mut agg = CandleAggregator::new("ctp", "rb2601", KlineInterval::M1);
        // 10:00 桶并墙钟定稿。
        agg.on_tick(&tick(20261027, 600_000, 3800, 10, 0, 1));
        assert_eq!(
            agg.finalize_if_due(660_000).map(|b| b.start_ms),
            Some(600_000)
        );
        // 10:05（900_000）正常 tick 开在途桶。
        assert!(agg
            .on_tick(&tick(20261027, 900_000, 3810, 20, 0, 2))
            .is_none());
        // 乱序来一笔 10:02（720_000，空窗）：不产棒，且 10:05 在途桶必须存活。
        assert!(agg
            .on_tick(&tick(20261027, 720_000, 3805, 15, 0, 2))
            .is_none());
        // 到 10:06 边界定稿的仍是 10:05 桶（未被 10:02 覆盖）。
        let b = agg
            .finalize_if_due(960_000)
            .expect("10:05 在途桶应完好并到点定稿");
        assert_eq!(b.start_ms, 900_000);
        // 量守恒：基线 10→20，乱序笔 15（更小，乱序）不计入 → 10:05 量 10。
        assert_eq!(b.volume, 10);
    }

    // ===== 零成交快照不参与 OHLC（方案 B：延续价出棒）=====

    /// 分钟/秒 → 当日毫秒（与 update_ms 同坐标系）。
    fn m_of(mm: i64, sec: i64) -> i64 {
        mm * 60_000 + sec * 1_000
    }

    // 上一分钟已有成交（建立累计量基线）后，新桶首笔零成交快照不得提前钉住 open；
    // 只有首个真实成交 tick 才初始化 OHLC。
    #[test]
    fn snapshot_before_first_trade_does_not_set_open() {
        const DAY: u32 = 20261030;
        let mut agg = CandleAggregator::new("ctp", "ag2706", KlineInterval::M1);
        // 09:00:59 真实成交（累计 47，价 14835）：建立基线。
        agg.on_tick(&tick(DAY, m_of(540, 59), 14835, 47, 0, 1));
        // 09:01:00 零成交快照：触发 09:00 桶边界定稿，但自身不得计入新桶 OHLC。
        let boundary = agg
            .on_tick(&tick(DAY, m_of(541, 0), 14840, 47, 0, 1))
            .expect("09:00 桶边界定稿");
        assert_eq!(boundary.start_ms, m_of(540, 0));
        // 09:01:05 真实成交：累计 48、价 14850。
        assert!(agg
            .on_tick(&tick(DAY, m_of(541, 5), 14850, 48, 0, 2))
            .is_none());

        let bar = agg.flush().expect("09:01 桶");
        assert_eq!(bar.start_ms, m_of(541, 0));
        assert_eq!(bar.open, dec(14850));
        assert_eq!(bar.high, dec(14850));
        assert_eq!(bar.low, dec(14850));
        assert_eq!(bar.close, dec(14850));
    }

    // 成交流之间夹入偏离快照价：high/low 只由真实成交 tick 决定。
    #[test]
    fn snapshot_prices_never_enter_high_low() {
        const DAY: u32 = 20261030;
        let mut agg = CandleAggregator::new("ctp", "ag2706", KlineInterval::M1);
        // 上一分钟成交建立基线（累计 100）。
        agg.on_tick(&tick(DAY, m_of(540, 59), 1000, 100, 0, 1));
        // 首笔成交 1010（累计 110）：初始化 OHLC。
        agg.on_tick(&tick(DAY, m_of(541, 5), 1010, 110, 0, 2));
        // 快照尖峰 1050（累计不变）：不得进入 high。
        agg.on_tick(&tick(DAY, m_of(541, 10), 1050, 110, 0, 3));
        // 快照下探 990（累计不变）：不得进入 low。
        agg.on_tick(&tick(DAY, m_of(541, 40), 990, 110, 0, 4));
        // 再成交 1005（累计 120）：更新 low/close。
        agg.on_tick(&tick(DAY, m_of(541, 50), 1005, 120, 0, 5));

        let bar = agg.flush().expect("09:01 桶");
        assert_eq!(bar.open, dec(1010));
        assert_eq!(bar.high, dec(1010));
        assert_eq!(bar.low, dec(1005));
        assert_eq!(bar.close, dec(1005));
        // 量仍按累计差分：120 − 100 = 20（快照不贡献量）。
        assert_eq!(bar.volume, 20);
    }

    // 整分钟零成交：仍出一根 volume=0 的 bar，OHLC 取上一根已定稿 bar 的收盘延续价。
    #[test]
    fn zero_trade_bucket_emits_carry_over_bar() {
        const DAY: u32 = 20261030;
        let mut agg = CandleAggregator::new("ctp", "ag2706", KlineInterval::M1);
        // 09:00 桶有成交：close 1000，墙钟定稿。
        agg.on_tick(&tick(DAY, m_of(540, 50), 1000, 100, 0, 1));
        let b0 = agg.finalize_if_due(m_of(541, 0)).expect("09:00 定稿");
        assert_eq!(b0.close, dec(1000));

        // 09:01 整分钟仅快照（累计保持 100、价格波动）。
        agg.on_tick(&tick(DAY, m_of(541, 10), 1003, 100, 0, 2));
        agg.on_tick(&tick(DAY, m_of(541, 40), 1005, 100, 0, 3));
        let b1 = agg.finalize_if_due(m_of(542, 0)).expect("09:01 定稿");

        assert_eq!(b1.volume, 0);
        assert_eq!(b1.quote_volume, dec(0));
        assert_eq!(b1.open, dec(1000));
        assert_eq!(b1.high, dec(1000));
        assert_eq!(b1.low, dec(1000));
        assert_eq!(b1.close, dec(1000));
        // 持仓仍取桶末快照值。
        assert_eq!(bar_oi(&b1), dec(3));
    }

    // 当日首桶纯快照（无上一根 close）：退化为该桶最新快照价。
    #[test]
    fn zero_trade_first_bucket_falls_back_to_last_price() {
        const DAY: u32 = 20261030;
        let mut agg = CandleAggregator::new("ctp", "ag2706", KlineInterval::M1);
        // 累计量为 0（当日尚无成交），两笔快照。
        agg.on_tick(&tick(DAY, m_of(541, 10), 14800, 0, 0, 1));
        agg.on_tick(&tick(DAY, m_of(541, 40), 14810, 0, 0, 2));

        let bar = agg.flush().expect("首桶");
        assert_eq!(bar.volume, 0);
        assert_eq!(bar.open, dec(14810));
        assert_eq!(bar.high, dec(14810));
        assert_eq!(bar.low, dec(14810));
        assert_eq!(bar.close, dec(14810));
    }

    // 回归：稀疏成交下墙钟 sweep 仍按时收口零成交桶，且下一根成交桶正常续接。
    #[test]
    fn sparse_trades_still_finalize_on_wallclock() {
        const DAY: u32 = 20261030;
        let mut agg = CandleAggregator::new("ctp", "ag2706", KlineInterval::M1);
        // 09:00 成交 1000（累计 100）→ 墙钟定稿。
        agg.on_tick(&tick(DAY, m_of(540, 59), 1000, 100, 0, 1));
        let b0 = agg.finalize_if_due(m_of(541, 0)).expect("09:00 定稿");
        assert_eq!(b0.close, dec(1000));

        // 09:01 仅快照 → 墙钟到点仍出延续价 bar。
        agg.on_tick(&tick(DAY, m_of(541, 10), 1004, 100, 0, 2));
        let b1 = agg.finalize_if_due(m_of(542, 0)).expect("09:01 定稿");
        assert_eq!(b1.start_ms, m_of(541, 0));
        assert_eq!(b1.close, dec(1000));
        assert_eq!(b1.volume, 0);

        // 09:02 成交 1012（累计 105）：新桶首笔即成交，open=1012，量 5。
        agg.on_tick(&tick(DAY, m_of(542, 5), 1012, 105, 0, 3));
        let b2 = agg.flush().expect("09:02 桶");
        assert_eq!(b2.open, dec(1012));
        assert_eq!(b2.close, dec(1012));
        assert_eq!(b2.volume, 5);
    }

    // 跨交易日累计量归零（50000 → 0）：零量首笔按快照处理，不误判为成交；
    // 随后累计量转正的 tick 才初始化 OHLC，且量差分不为负。
    #[test]
    fn cross_day_cum_reset_not_treated_as_trade() {
        const D1: u32 = 20261029;
        const D2: u32 = 20261030;
        let mut agg = CandleAggregator::new("ctp", "ag2706", KlineInterval::M1);
        // 前一日 14:59 成交，累计 50000；墙钟定稿。
        agg.on_tick(&tick(D1, m_of(899, 0), 70000, 50_000, 0, 100));
        let d1 = agg.finalize_if_due(m_of(900, 0)).expect("前日定稿");
        assert_eq!(d1.trading_day, D1);

        // 次日 09:00:00 累计归零的快照：不得把 70010 钉成 open。
        assert!(agg
            .on_tick(&tick(D2, m_of(540, 0), 70010, 0, 0, 101))
            .is_none());
        // 次日 09:00:05 首笔成交（累计 3）：初始化 OHLC。
        assert!(agg
            .on_tick(&tick(D2, m_of(540, 5), 70020, 3, 0, 102))
            .is_none());

        let bar = agg.flush().expect("次日桶");
        assert_eq!(bar.trading_day, D2);
        assert_eq!(bar.open, dec(70020));
        assert_eq!(bar.high, dec(70020));
        assert_eq!(bar.low, dec(70020));
        assert_eq!(bar.close, dec(70020));
        // 量 = 3 − 次日基线 0 = 3，绝不为负。
        assert_eq!(bar.volume, 3);
    }

    // 跨交易日 + 整分钟零成交：不得沿用上一交易日收盘价，必须退化为本桶最新快照价。
    #[test]
    fn cross_day_zero_trade_bucket_uses_snapshot_not_prev_day_close() {
        const D1: u32 = 20261029;
        const D2: u32 = 20261030;
        let mut agg = CandleAggregator::new("ctp", "ag2706", KlineInterval::M1);
        // 前一日 14:59 成交，收盘 70000；墙钟定稿后 last_close=(D1, 70000)。
        agg.on_tick(&tick(D1, m_of(899, 0), 70000, 50_000, 0, 100));
        let d1 = agg.finalize_if_due(m_of(900, 0)).expect("前日定稿");
        assert_eq!(d1.close, dec(70000));

        // 次日 09:00 整分钟仅零成交快照（累计归零），价格 70010 → 70020。
        agg.on_tick(&tick(D2, m_of(540, 10), 70010, 0, 0, 101));
        agg.on_tick(&tick(D2, m_of(540, 40), 70020, 0, 0, 102));
        let bar = agg.finalize_if_due(m_of(541, 0)).expect("次日零成交桶");

        assert_eq!(bar.trading_day, D2);
        assert_eq!(bar.volume, 0);
        assert_eq!(bar.quote_volume, dec(0));
        // 关键：跨日不延续 D1 的 70000，退化为本桶最新快照价 70020。
        assert_eq!(bar.open, dec(70020));
        assert_eq!(bar.high, dec(70020));
        assert_eq!(bar.low, dec(70020));
        assert_eq!(bar.close, dec(70020));
        assert_eq!(bar_oi(&bar), dec(102));
    }

    // 连续多个整分钟零成交：每根都延续同一价格（延续链），不得被桶内快照价改写。
    #[test]
    fn consecutive_zero_trade_buckets_chain_prior_close() {
        const DAY: u32 = 20261030;
        let mut agg = CandleAggregator::new("ctp", "ag2706", KlineInterval::M1);
        // 09:00 成交 close=1000，墙钟定稿。
        agg.on_tick(&tick(DAY, m_of(540, 50), 1000, 100, 0, 1));
        let b0 = agg.finalize_if_due(m_of(541, 0)).expect("09:00 定稿");
        assert_eq!(b0.close, dec(1000));

        // 09:01 仅快照（价 1003）：出延续价 bar，close 仍为 1000。
        agg.on_tick(&tick(DAY, m_of(541, 10), 1003, 100, 0, 2));
        let b1 = agg.finalize_if_due(m_of(542, 0)).expect("09:01 定稿");
        assert_eq!(b1.start_ms, m_of(541, 0));
        assert_eq!(b1.volume, 0);
        assert_eq!(b1.close, dec(1000));

        // 09:02 仍仅快照（价 1007）：应继续延续 1000（链式），而不是改写为 1007。
        agg.on_tick(&tick(DAY, m_of(542, 10), 1007, 100, 0, 3));
        let b2 = agg.finalize_if_due(m_of(543, 0)).expect("09:02 定稿");
        assert_eq!(b2.start_ms, m_of(542, 0));
        assert_eq!(b2.volume, 0);
        assert_eq!(b2.quote_volume, dec(0));
        assert_eq!(b2.open, dec(1000));
        assert_eq!(b2.high, dec(1000));
        assert_eq!(b2.low, dec(1000));
        assert_eq!(b2.close, dec(1000));
    }
}
