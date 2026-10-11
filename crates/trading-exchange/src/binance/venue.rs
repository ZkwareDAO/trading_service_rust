//! Binance U 本位永续的新适配器主体（旁路，尚未接入线上 `Exchange` 装配）。
//!
//! * [`BinanceRuleFetcher`]：交易所主体、无认证、全用户共享，实现 [`RuleFetcher`]，
//!   从 `/fapi/v1/exchangeInfo` 拉公开规则写入共享 [`OrderRuleTable`]；
//! * [`BinanceUser`]：用户主体（一套 key/secret），实现 [`ExchangeApi`]，下单前先
//!   [`prepare_order`] 做本地校验/对齐，撤/查单用 [`route_order_ref`] 单号路由。
//!
//! 见 `docs/扩展交易所.md` 第二节、`docs/交易规则.md` 第二节。
//!
//! 旁路实现：尚未接入线上 `Exchange` 装配，非测试构建暂无消费方（ignored 冒烟测试
//! 除外），允许 dead_code；切换线上路径后移除。
#![allow(dead_code)]

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use binance_sdk::common::errors::ConnectorError;
use binance_sdk::config::ConfigurationRestApi;
use binance_sdk::derivatives_trading_usds_futures::rest_api::PositionInformationV2ResponseInner;
use binance_sdk::derivatives_trading_usds_futures::rest_api::{
    CancelOrderParams, ChangeInitialLeverageParams, GetCurrentPositionModeParams, NewOrderParams,
    NewOrderReduceOnlyEnum, NewOrderSideEnum, NewOrderTimeInForceEnum, NewOrderTypeEnum,
    PositionInformationV2Params, QueryOrderParams, RestApi as SdkRestApi,
};
use binance_sdk::derivatives_trading_usds_futures::DerivativesTradingUsdsFuturesRestApi;
use futures_util::stream::{self, BoxStream, StreamExt};
use reqwest::Client;
use rust_decimal::Decimal;
use tokio_tungstenite::tungstenite::Message;
use trading_core::order_rule::OrderRuleTable;
use trading_models::{InstrumentType, OrderStatus, PositionMode, Side};

use crate::api::{
    AccountDirty, AccountEvent, AccountEventStream, ExchangeApi, OrderAck, OrderLookup, OrderRef,
    OrderState, PlaceErrorKind, PlaceOrderRequest, PositionInfo, PriceBasis, ReferencePrice,
    RuleFetcher,
};
use crate::backoff::ReconnectBackoff;
use crate::binance::prep::{
    can_send_reduce_only, hedge_position_side, map_position_side, prepare_order, route_order_ref,
    OrderKey, PreparedOrder,
};
use crate::binance::rules::{parse_exchange_info, BinanceMarket};
use crate::heartbeat::{Heartbeat, HeartbeatAction};

const EXCHANGE: &str = "binance";

/// 签名请求的 recvWindow（毫秒）：放宽本机时钟与币安服务器的偏差，避免
/// `-1021 Timestamp outside recvWindow`。币安允许上限 60000，取 10s 兼顾
/// 时效与抗延迟（根治仍建议系统开启 NTP）。
const RECV_WINDOW_MS: i64 = 10_000;

fn fapi_base(testnet: bool) -> &'static str {
    if testnet {
        "https://testnet.binancefuture.com"
    } else {
        "https://fapi.binance.com"
    }
}

fn parse_dec(raw: &str) -> anyhow::Result<Decimal> {
    Decimal::from_str(raw.trim()).map_err(|e| anyhow::anyhow!("invalid decimal `{raw}`: {e}"))
}

/// Binance 状态串 -> 领域状态；初始挂单统一归到 NEW。
fn map_status(raw: &str) -> OrderStatus {
    match raw.trim().to_ascii_uppercase().as_str() {
        "NEW" => OrderStatus::New,
        "PARTIALLY_FILLED" => OrderStatus::Partial,
        "FILLED" => OrderStatus::Filled,
        "CANCELED" | "CANCELLED" => OrderStatus::Canceled,
        "EXPIRED" | "REJECTED" => OrderStatus::Expired,
        _ => OrderStatus::Unknown,
    }
}

// map_position_side 已收口到 prep.rs（下单/回报/查询共用）。

/// 可选字符串字段解析 Decimal；None/空由调用方默认，非法串带字段名上抛。
fn parse_opt_dec(field: &str, raw: Option<&str>) -> anyhow::Result<Option<Decimal>> {
    raw.map(|s| parse_dec(s.trim_start_matches('+')))
        .transpose()
        .map_err(|e| anyhow::anyhow!("binance 持仓字段 `{field}` 解析失败: {e}"))
}

/// 解析杠杆字符串（"10"->10）；缺失/非法返回 None，不阻断整个快照。
fn parse_leverage(raw: Option<&str>) -> Option<u64> {
    raw.and_then(|s| s.trim().parse::<u64>().ok())
}

/// 币安「订单不存在」错误码（-2013 Order does not exist）。
const ERR_ORDER_NOT_EXISTS: i64 = -2013;

/// 从 [`ConnectorError`] 取出交易所错误码（各带码变体；其余为 None）。
fn connector_code(ce: &ConnectorError) -> Option<i64> {
    match ce {
        ConnectorError::ConnectorClientError { code, .. }
        | ConnectorError::UnauthorizedError { code, .. }
        | ConnectorError::ForbiddenError { code, .. }
        | ConnectorError::TooManyRequestsError { code, .. }
        | ConnectorError::RateLimitBanError { code, .. }
        | ConnectorError::NotFoundError { code, .. }
        | ConnectorError::BadRequestError { code, .. } => *code,
        _ => None,
    }
}

/// 直接按 [`ConnectorError`] 分类：code=-2013 → NotExists，其余 → Unavailable。
fn classify_connector(ce: &ConnectorError) -> OrderLookup {
    if connector_code(ce) == Some(ERR_ORDER_NOT_EXISTS) {
        OrderLookup::NotExists
    } else {
        OrderLookup::Unavailable
    }
}

/// 把查单错误（anyhow 链）分类为 [`OrderLookup`]：
/// * 错误链中存在 [`ConnectorError`] 且 code = -2013 → [`OrderLookup::NotExists`]
///   （交易所明确应答订单不存在）；
/// * 其余任何错误（网络/超时/签名/解析）→ [`OrderLookup::Unavailable`]
///   （查询本身失败，不代表订单不存在）。
fn classify_query_error(err: &anyhow::Error) -> OrderLookup {
    for cause in err.chain() {
        if let Some(ce) = cause.downcast_ref::<ConnectorError>() {
            return classify_connector(ce);
        }
    }
    OrderLookup::Unavailable
}

/// 币安下单错误分类：错误链中存在 [`ConnectorError::BadRequestError`]（HTTP 400
/// 业务应答，如 -2010 ReduceOnly 被拒）→ [`PlaceErrorKind::Rejected`]——交易所
/// 明确拒绝、订单确定未受理；其余错误（超时 / NetworkError / 5xx / 429 /
/// Unauthorized / 解析异常）→ [`PlaceErrorKind::Indeterminate`]，应答不确定。
fn classify_place_error_chain(err: &anyhow::Error) -> PlaceErrorKind {
    for cause in err.chain() {
        if let Some(ConnectorError::BadRequestError { .. }) = cause.downcast_ref::<ConnectorError>()
        {
            return PlaceErrorKind::Rejected;
        }
    }
    PlaceErrorKind::Indeterminate
}

/// 一行 v2 positionRisk -> 统一快照；symbol 缺失或 0 量行返回 None（过滤）。
///
/// v2 不直接给持仓 notional（maxNotionalValue 是杠杆限额而非名义价值），
/// 故按 markPrice × |qty| 本地计算；margin 取非零 isolatedMargin（交叉持仓为
/// "0"->None，由宿主按 notional/leverage 自算）。
fn map_v2_position(p: PositionInformationV2ResponseInner) -> anyhow::Result<Option<PositionInfo>> {
    let symbol = match p.symbol.as_deref().map(str::trim) {
        Some(s) if !s.is_empty() => s.to_string(),
        _ => return Ok(None),
    };
    let signed = parse_opt_dec("positionAmt", p.position_amt.as_deref())?.unwrap_or(Decimal::ZERO);
    if signed == Decimal::ZERO {
        return Ok(None); // 0 量行：从未交易或已平仓，滤掉。
    }
    let mark = parse_opt_dec("markPrice", p.mark_price.as_deref())?;
    let notional = mark.map(|m| (m * signed.abs()).abs());
    // 逐仓保证金；"0"/缺失视为交叉或未提供，留给宿主计算。
    let margin = parse_opt_dec("isolatedMargin", p.isolated_margin.as_deref())?
        .filter(|d| *d > Decimal::ZERO);
    Ok(Some(PositionInfo {
        instrument: symbol,
        instrument_type: InstrumentType::Futures,
        position_side: map_position_side(p.position_side.as_deref()),
        signed_quantity: signed,
        entry_price: parse_opt_dec("entryPrice", p.entry_price.as_deref())?
            .unwrap_or(Decimal::ZERO),
        mark_price: mark,
        unrealized_pnl: parse_opt_dec("unRealizedProfit", p.un_realized_profit.as_deref())?,
        leverage: parse_leverage(p.leverage.as_deref()),
        notional,
        margin,
        update_ms: p.update_time,
    }))
}

// =====================================================================
// 交易所主体：公开规则抓取
// =====================================================================

/// 无认证规则抓取器；进程内每市场一个，所有用户共享。
pub struct BinanceRuleFetcher {
    http: Client,
    testnet: bool,
    market: BinanceMarket,
}

impl BinanceRuleFetcher {
    pub fn new(testnet: bool) -> Self {
        Self {
            http: Client::new(),
            testnet,
            market: BinanceMarket::UsdmFutures,
        }
    }

    /// 交易所级公开规则抓取恒定正式网（进程共享规则表，不随账户区分环境）。
    pub fn new_prod() -> Self {
        Self::new(false)
    }

    async fn fetch_raw(&self) -> anyhow::Result<String> {
        // 当前只接 U 本位永续；现货接入时按 market 切 /api/v3/exchangeInfo。
        let url = format!("{}/fapi/v1/exchangeInfo", fapi_base(self.testnet));
        let resp = self.http.get(&url).send().await?;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            anyhow::bail!("binance exchangeInfo HTTP {status}: {text}");
        }
        Ok(text)
    }
}

#[async_trait]
impl RuleFetcher for BinanceRuleFetcher {
    fn exchange(&self) -> &str {
        EXCHANGE
    }

    async fn sync_rules(&self, table: &OrderRuleTable) -> anyhow::Result<usize> {
        let raw = self.fetch_raw().await?;
        let rows = parse_exchange_info(self.market, &raw)?;
        let n = rows.len();
        for row in rows {
            table.put(row);
        }
        Ok(n)
    }

    async fn ensure_rule(
        &self,
        table: &OrderRuleTable,
        instrument_type: InstrumentType,
        instrument: &str,
    ) -> anyhow::Result<Arc<trading_models::OrderRule>> {
        let key = trading_models::OrderRuleKey::new(EXCHANGE, instrument_type, instrument);
        if let Some(row) = table.get(&key) {
            return Ok(row);
        }
        // 未命中：全量补拉一次再读（Binance/HL 口径；启动同步后理论上不进此分支）。
        self.sync_rules(table).await?;
        table
            .get(&key)
            .ok_or_else(|| anyhow::anyhow!("binance 规则缺失：{instrument} ({instrument_type:?})"))
    }
}

// =====================================================================
// 用户主体：REST 交易/查询
// =====================================================================

/// 一个「用户 × Binance U 本位永续」的 REST 主体，持有签名 SDK client 与共享规则表。
pub struct BinanceUser {
    rest: Arc<SdkRestApi>,
    http: Client,
    rules: Arc<OrderRuleTable>,
    fetcher: BinanceRuleFetcher,
    #[allow(dead_code)] // 装配语义保留，后续环境选择/日志使用
    mode: PositionMode,
    #[allow(dead_code)]
    testnet: bool,
}

impl BinanceUser {
    /// 用账户 key/secret 构造（日志与返回均不包含密钥）。
    pub fn with_credentials(
        api_key: &str,
        api_secret: &str,
        testnet: bool,
        mode: PositionMode,
        rules: Arc<OrderRuleTable>,
    ) -> anyhow::Result<Self> {
        let conf = ConfigurationRestApi::builder()
            .api_key(api_key.to_string())
            .api_secret(api_secret.to_string())
            .build()?;
        let rest: SdkRestApi = if testnet {
            DerivativesTradingUsdsFuturesRestApi::testnet(conf)
        } else {
            DerivativesTradingUsdsFuturesRestApi::production(conf)
        };
        Ok(Self {
            rest: Arc::new(rest),
            http: Client::new(),
            rules,
            // 规则表是进程级共享表，恒取正式网公开规则，不被账户测试网污染。
            fetcher: BinanceRuleFetcher::new_prod(),
            mode,
            testnet,
        })
    }

    /// 用已缓存的签名 SDK client 构造（装配路径：复用 BinanceFuturesClientCache，
    /// 不重复建 client）。
    pub fn from_sdk(
        rest: Arc<SdkRestApi>,
        rules: Arc<OrderRuleTable>,
        mode: PositionMode,
        testnet: bool,
    ) -> Self {
        Self {
            rest,
            http: Client::new(),
            rules,
            // 规则表是进程级共享表，恒取正式网公开规则，不被账户测试网污染。
            fetcher: BinanceRuleFetcher::new_prod(),
            mode,
            testnet,
        }
    }

    fn side_enum(side: Side) -> NewOrderSideEnum {
        match side {
            Side::Buy => NewOrderSideEnum::Buy,
            Side::Sell => NewOrderSideEnum::Sell,
        }
    }

    fn tif_enum(tif: &str) -> NewOrderTimeInForceEnum {
        match tif {
            "IOC" => NewOrderTimeInForceEnum::Ioc,
            "FOK" => NewOrderTimeInForceEnum::Fok,
            _ => NewOrderTimeInForceEnum::Gtc,
        }
    }
}

/// 下单入口兜底：返回一份标的大写化的请求副本（Binance 现货/合约标的一律大写）。
fn uppercase_instrument(req: &PlaceOrderRequest) -> PlaceOrderRequest {
    let mut owned = req.clone();
    owned.instrument = owned.instrument.trim().to_ascii_uppercase();
    owned
}

#[async_trait]
impl ExchangeApi for BinanceUser {
    fn exchange(&self) -> &str {
        EXCHANGE
    }

    async fn place_order(&self, req: &PlaceOrderRequest) -> anyhow::Result<OrderAck> {
        // 入口兜底：Binance 标的一律大写（宿主通常已归一，此处防止其它调用方漏归一）。
        let req = uppercase_instrument(req);
        // 1) 规则就绪 + 本地校验/对齐；规则缺失或违规直接失败，不把非法单下发。
        let row = self
            .fetcher
            .ensure_rule(&self.rules, req.instrument_type, &req.instrument)
            .await?;
        let prepared = prepare_order(Some(&row), &req).map_err(|e| {
            anyhow::anyhow!(
                "binance 下单校验失败 {}/{}: {e:?}",
                req.instrument,
                req.client_order_id
            )
        })?;

        let side = Self::side_enum(req.side);
        // NewOrder builder 在构造时即确定订单类型（无独立 order_type setter）。
        let order_type = match &prepared {
            PreparedOrder::Limit(_) => NewOrderTypeEnum::Limit,
            PreparedOrder::Market { .. } => NewOrderTypeEnum::Market,
        };
        let mut builder = NewOrderParams::builder(req.instrument.clone(), side, order_type)
            .new_client_order_id(Some(req.client_order_id.clone()))
            .recv_window(Some(RECV_WINDOW_MS));

        match prepared {
            PreparedOrder::Limit(l) => {
                let price = parse_dec(&l.price)?;
                let qty = parse_dec(&l.quantity)?;
                builder = builder
                    .price(Some(price))
                    .quantity(Some(qty))
                    .time_in_force(Some(Self::tif_enum(l.time_in_force)));
            }
            PreparedOrder::Market { quantity } => {
                let qty = parse_dec(&quantity)?;
                builder = builder.quantity(Some(qty));
            }
        }

        // 注意：`req.reduce_only` 是系统**内部**的"这是平仓"语义标记，三条平仓路径
        // 都统一带它；但它不等于发给币安的 reduceOnly 参数。币安规则下两种模式互斥：
        //   · 单向 one-way：不发 positionSide，**发** reduceOnly=true（靠它只减仓）；
        //   · 双向 hedge：**发** positionSide=LONG/SHORT 锁定被平那条仓，**绝不发**
        //     reduceOnly（hedge 下二者同传会被交易所拒；平仓方向已由 positionSide
        //     表达，不会反手）。
        // 故下面：positionSide 仅 hedge 发；reduceOnly 仅单向发。

        // hedge：开仓按 side、平仓按持仓反向发 LONG/SHORT（平仓不变开仓）；
        // 单向：不发 positionSide。
        if let Some(ps) = hedge_position_side(self.mode, req.side, req.reduce_only) {
            builder = builder.position_side(Some(ps.to_string()));
        }
        // reduceOnly 仅单向可发；hedge 下忽略（交易所会拒），由多空方向表达平仓。
        if req.reduce_only && can_send_reduce_only(self.mode) {
            builder = builder.reduce_only(Some(NewOrderReduceOnlyEnum::True));
        } else if req.reduce_only {
            tracing::debug!(
                instrument = %req.instrument,
                "hedge 模式不发送 reduceOnly，忽略该内部标记（以 positionSide 平仓）"
            );
        }

        let resp = self.rest.new_order(builder.build()?).await?;
        let data = resp.data().await?;
        Ok(OrderAck {
            accepted: true,
            client_order_id: data
                .client_order_id
                .unwrap_or_else(|| req.client_order_id.clone()),
            exchange_order_id: data.order_id.map(|id| id.to_string()),
            status: OrderStatus::New, // 初始状态统一 NEW
            message: None,
            placed_type: None,
            placed_price: None,
            placed_quantity: None,
        })
    }

    /// 币安能识别 HTTP 400 明确业务拒绝：覆盖默认分类。
    fn classify_place_error(&self, err: &anyhow::Error) -> PlaceErrorKind {
        classify_place_error_chain(err)
    }

    async fn cancel_order(&self, instrument: &str, order: &OrderRef) -> anyhow::Result<OrderAck> {
        // 入口兜底：Binance 标的一律大写。
        let instrument = instrument.trim().to_ascii_uppercase();
        let route = route_order_ref(order);

        let build_params = |key: &OrderKey| -> anyhow::Result<CancelOrderParams> {
            let builder = CancelOrderParams::builder(instrument.to_string());
            match key {
                OrderKey::ClientOrderId(cid) => {
                    Ok(builder.orig_client_order_id(Some(cid.clone())).build()?)
                }
                // Binance 单号可解析为 i64（带前缀形态归 Deribit 适配器处理）。
                OrderKey::ExchangeOrderId(eid) => {
                    Ok(builder.order_id(Some(eid.parse()?)).build()?)
                }
            }
        };

        // 首选 client_order_id；失败且有交易所单号时按 orderId 兜底重试一次。
        let resp = match self.rest.cancel_order(build_params(&route.primary)?).await {
            Ok(r) => r,
            Err(primary_err) => match &route.fallback {
                Some(fb @ OrderKey::ExchangeOrderId(_)) => {
                    tracing::warn!(
                        instrument,
                        client_order_id = %order.client_order_id,
                        error = %primary_err,
                        "按 client_order_id 撤单失败，回退 orderId 重试"
                    );
                    self.rest.cancel_order(build_params(fb)?).await?
                }
                _ => return Err(primary_err),
            },
        };

        let data = resp.data().await?;
        Ok(OrderAck {
            accepted: true,
            client_order_id: data
                .client_order_id
                .unwrap_or_else(|| order.client_order_id.clone()),
            exchange_order_id: data.order_id.map(|id| id.to_string()),
            status: OrderStatus::Canceled,
            message: None,
            placed_type: None,
            placed_price: None,
            placed_quantity: None,
        })
    }

    async fn query_order(&self, instrument: &str, order: &OrderRef) -> OrderLookup {
        // 入口兜底：Binance 标的一律大写。
        let instrument = instrument.trim().to_ascii_uppercase();
        let build_params = |key: &OrderKey| -> anyhow::Result<QueryOrderParams> {
            let builder = QueryOrderParams::builder(instrument.to_string());
            match key {
                OrderKey::ClientOrderId(cid) => {
                    Ok(builder.orig_client_order_id(Some(cid.clone())).build()?)
                }
                OrderKey::ExchangeOrderId(eid) => {
                    Ok(builder.order_id(Some(eid.parse()?)).build()?)
                }
            }
        };

        let route = route_order_ref(order);
        // build_params 本身失败（参数构造）属查询前置错误，按 Unavailable。
        let primary_params = match build_params(&route.primary) {
            Ok(p) => p,
            Err(_) => return OrderLookup::Unavailable,
        };
        let resp = match self.rest.query_order(primary_params).await {
            Ok(r) => r,
            Err(primary_err) => {
                // 首选号即明确 -2013：无需再试 fallback，直接 NotExists。
                if matches!(classify_query_error(&primary_err), OrderLookup::NotExists) {
                    return OrderLookup::NotExists;
                }
                match &route.fallback {
                    Some(fb) => {
                        let fb_params = match build_params(fb) {
                            Ok(p) => p,
                            Err(_) => return OrderLookup::Unavailable,
                        };
                        match self.rest.query_order(fb_params).await {
                            Ok(r) => r,
                            Err(fb_err) => {
                                // 两种单号都失败：按 fallback 错误分类（-2013→NotExists，
                                // 其余→Unavailable）。
                                return classify_query_error(&fb_err);
                            }
                        }
                    }
                    None => return classify_query_error(&primary_err),
                }
            }
        };
        let d = match resp.data().await {
            Ok(d) => d,
            // data() 的错误即 ConnectorError：按 code 分类（-2013→NotExists）。
            Err(e) => return classify_connector(&e),
        };
        // 数量/均价解析失败时按缺省（0 / None）处理，不致整体查单失败。
        let filled_quantity = d
            .executed_qty
            .as_deref()
            .map(parse_dec)
            .and_then(Result::ok)
            .unwrap_or(Decimal::ZERO);
        let average_price = d.avg_price.as_deref().map(parse_dec).and_then(Result::ok);
        OrderLookup::Found(OrderState {
            client_order_id: d.client_order_id,
            exchange_order_id: d.order_id.map(|id| id.to_string()),
            instrument: d.symbol.unwrap_or_else(|| instrument.to_string()),
            status: map_status(&d.status.unwrap_or_default()),
            filled_quantity,
            average_price,
            position_side: map_position_side(d.position_side.as_deref()),
            venue_extra: None,
        })
    }

    async fn get_positions(&self) -> anyhow::Result<Vec<PositionInfo>> {
        // 用 v2（/fapi/v2/positionRisk）：v3 响应不含 leverage，而 v2 一次返回
        // positionSide/entryPrice/markPrice/unRealizedProfit/leverage/isolatedMargin/updateTime。
        let params = PositionInformationV2Params::builder()
            .recv_window(Some(RECV_WINDOW_MS))
            .build()?;
        let rows = self
            .rest
            .position_information_v2(params)
            .await?
            .data()
            .await?;
        let mut out = Vec::new();
        for row in rows {
            if let Some(info) = map_v2_position(row)? {
                out.push(info);
            }
        }
        Ok(out)
    }

    async fn set_leverage(&self, instrument: &str, leverage: u32) -> anyhow::Result<()> {
        let params = ChangeInitialLeverageParams::builder(instrument.to_string(), leverage as i64)
            .build()?;
        self.rest.change_initial_leverage(params).await?;
        Ok(())
    }

    async fn get_reference_price(&self, instrument: &str) -> anyhow::Result<ReferencePrice> {
        // 公开端点无需签名；直接用 reqwest，避免依赖 SDK 的私有响应模型。
        #[derive(serde::Deserialize)]
        struct PriceResp {
            price: String,
        }
        let url = format!(
            "{}/fapi/v1/ticker/price?symbol={}",
            fapi_base(self.testnet),
            instrument
        );
        let resp: PriceResp = self.http.get(&url).send().await?.json().await?;
        Ok(ReferencePrice {
            instrument: instrument.to_string(),
            basis: PriceBasis::Last, // 币安口径=最新成交价
            price: parse_dec(&resp.price)?,
        })
    }

    /// 只读查询交易所实际持仓模式（/fapi/v1/positionSide/dual）：供启动比对本地配置，
    /// 不一致只告警，绝不自动修改用户交易所设置。
    async fn query_position_mode(&self) -> anyhow::Result<Option<PositionMode>> {
        let params = GetCurrentPositionModeParams::builder()
            .recv_window(Some(RECV_WINDOW_MS))
            .build()?;
        let data = self
            .rest
            .get_current_position_mode(params)
            .await?
            .data()
            .await?;
        // dualSidePosition：true=hedge；缺字段保守按 one-way（不据此误判）。
        Ok(Some(if data.dual_side_position.unwrap_or(false) {
            PositionMode::Hedge
        } else {
            PositionMode::OneWay
        }))
    }
}

#[async_trait]
impl AccountEventStream for BinanceUser {
    /// 账户事件数据流：**不另起任务、不用 channel**，直接返回由 `stream::unfold`
    /// 驱动的 Stream——在状态转移里完成 listenKey 申请、WS 读帧、30 分钟 keepalive 与
    /// 退避重连（10s→30s→1/3/5min）。一条连接产出订单与账户两类事件；drop 流即停 IO。
    async fn subscribe_account_events(&self) -> anyhow::Result<BoxStream<'static, AccountEvent>> {
        let conn = AccountConn {
            rest: Arc::clone(&self.rest),
            testnet: self.testnet,
            ws: None,
            keepalive: None,
            backoff: ReconnectBackoff::new(),
            heartbeat: Heartbeat::new_default(),
        };
        Ok(Box::pin(stream::unfold(conn, |mut c| async move {
            let ev = c.next_event().await?; // None（永不）即结束流
            Some((ev, c))
        })))
    }
}

/// 私有流的 unfold 状态：持有一条已认证 WS 与 keepalive，断线则在状态内重连。
struct AccountConn {
    rest: Arc<SdkRestApi>,
    testnet: bool,
    ws: Option<crate::ws::WsStream>,
    keepalive: Option<tokio::time::Interval>,
    /// 重连退避阶梯（首次立即；失败/断线后 10s→30s→1/3/5min）。
    backoff: ReconnectBackoff,
    /// 应用层保活：主动 Ping + 空闲超时，防 TCP 半开挂起。
    heartbeat: Heartbeat,
}

impl AccountConn {
    /// 阻塞到产出下一个私有事件；断线自动重连，重连成功先补发一次全量账户脏信号。
    async fn next_event(&mut self) -> Option<AccountEvent> {
        use futures_util::SinkExt;

        loop {
            // 未连接：按需退避后（重新）建立 listenKey + WS。
            if self.ws.is_none() {
                if let Some(wait) = self.backoff.next_wait() {
                    tokio::time::sleep(wait).await;
                }
                match self.connect().await {
                    Ok(()) => {
                        self.backoff.reset();
                        tracing::info!(
                            testnet = self.testnet,
                            "binance account event stream connected"
                        );
                        // 启动 / 重连补一次全量对账信号（真值仍由消费方走 REST 拉取）。
                        return Some(AccountEvent::Account(AccountDirty::default()));
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "binance account event connect failed, backing off");
                        self.backoff.note_retry();
                        continue;
                    }
                }
            }

            let ws = self.ws.as_mut().expect("ws connected");
            let keepalive = self.keepalive.as_mut().expect("keepalive set");
            let hb_wait = self.heartbeat.deadline();
            tokio::select! {
                _ = keepalive.tick() => {
                    if let Err(e) = self.rest.keepalive_user_data_stream().await {
                        tracing::warn!(error = %e, "listenKey keepalive failed");
                    }
                }
                _ = tokio::time::sleep(hb_wait) => {
                    match self.heartbeat.advance(hb_wait) {
                        HeartbeatAction::Ping => {
                            // 发送失败说明 socket 已坏：teardown 后由外循环重连。
                            if let Err(e) = ws.send(Message::Ping(Default::default())).await {
                                tracing::warn!(error = %e, "ping send failed, reconnecting");
                                self.teardown();
                            }
                        }
                        HeartbeatAction::Timeout => {
                            tracing::warn!("binance account event stream idle timeout, reconnecting");
                            self.teardown();
                        }
                        HeartbeatAction::Wait => {}
                    }
                }
                frame = ws.next() => {
                    let Some(frame) = frame else {
                        self.teardown(); // 对端关闭
                        continue;
                    };
                    self.heartbeat.on_frame(); // 任意帧（含 Pong）证明存活
                    match frame {
                        Err(e) => {
                            tracing::warn!(error = %e, "binance account event ws error, reconnecting");
                            self.teardown();
                        }
                        Ok(Message::Ping(p)) => { let _ = ws.send(Message::Pong(p)).await; }
                        Ok(Message::Close(_)) => self.teardown(),
                        Ok(Message::Text(t)) => {
                            if let Some(ev) = self.on_text(&t) {
                                return Some(ev);
                            }
                        }
                        Ok(Message::Binary(b)) => {
                            if let Some(ev) = self.on_text(&String::from_utf8_lossy(&b)) {
                                return Some(ev);
                            }
                        }
                        Ok(_) => {}
                    }
                }
            }
        }
    }

    /// 处理一帧文本/二进制负载：listenKey 过期则拆掉连接触发重连；否则解析为私有事件。
    /// 非订单/账户事件返回 None；解析失败只告警不断流。
    fn on_text(&mut self, text: &str) -> Option<AccountEvent> {
        use crate::binance::mapping::{is_listen_key_expired, parse_account_event};
        // 原始账户事件帧 debug：逐帧可追溯（私有流频率低，全量 payload 可接受）。
        tracing::debug!(payload = %text, "binance account frame received");
        if is_listen_key_expired(text) {
            tracing::warn!("listenKey expired, recreating stream");
            self.teardown();
            return None;
        }
        match parse_account_event(text) {
            Ok(ev) => ev,
            Err(e) => {
                tracing::warn!(error = %e, payload = %text, "bad binance account event");
                None
            }
        }
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        let listen_key = self
            .rest
            .start_user_data_stream()
            .await?
            .data()
            .await?
            .listen_key
            .ok_or_else(|| anyhow::anyhow!("start_user_data_stream missing listenKey"))?;
        let ws_url = if self.testnet {
            format!("wss://stream.binancefuture.com/ws/{listen_key}")
        } else {
            format!("wss://fstream.binance.com/ws/{listen_key}")
        };
        let ws = crate::ws::connect(&ws_url).await?;
        // fapi listenKey 60 分钟有效，按 30 分钟续期。
        let mut keepalive = tokio::time::interval(Duration::from_secs(30 * 60));
        keepalive.tick().await; // 跳过首个即时 tick
        self.ws = Some(ws);
        self.keepalive = Some(keepalive);
        self.heartbeat.reset(); // 新连接重新计时空闲
        Ok(())
    }

    /// 连接结束（对端关闭/错误/listenKey 过期）：拆掉连接并排程下一档退避等待。
    fn teardown(&mut self) {
        self.ws = None;
        self.keepalive = None;
        self.backoff.note_retry();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::OrderPositionSide;
    use trading_models::OrderType;

    #[test]
    fn uppercase_instrument_normalizes_lowercase_entry() {
        let req = PlaceOrderRequest {
            instrument: " btcusdt ".into(),
            instrument_type: InstrumentType::Futures,
            side: Side::Buy,
            order_type: OrderType::Market,
            price: None,
            quantity: Decimal::ONE,
            reduce_only: false,
            position_side: OrderPositionSide::Both,
            time_in_force: None,
            client_order_id: "0x".to_string() + &"a".repeat(32),
        };
        let normalized = uppercase_instrument(&req);
        assert_eq!(normalized.instrument, "BTCUSDT");
        // 入参不被修改。
        assert_eq!(req.instrument, " btcusdt ");
    }

    /// 构造一行 v2 持仓应答，只给关心的字段（其余全 None）。
    #[allow(clippy::too_many_arguments)]
    fn v2_row(
        symbol: &str,
        side: Option<&str>,
        amt: Option<&str>,
        entry: Option<&str>,
        mark: Option<&str>,
        leverage: Option<&str>,
        isolated_margin: Option<&str>,
        update_ms: Option<i64>,
    ) -> PositionInformationV2ResponseInner {
        let s = |x: &str| Some(x.to_string());
        PositionInformationV2ResponseInner {
            symbol: s(symbol),
            position_side: side.map(str::to_string),
            position_amt: amt.map(str::to_string),
            entry_price: entry.map(str::to_string),
            mark_price: mark.map(str::to_string),
            un_realized_profit: s("5.5"),
            leverage: leverage.map(str::to_string),
            isolated_margin: isolated_margin.map(str::to_string),
            update_time: update_ms,
            ..Default::default()
        }
    }

    #[test]
    fn maps_one_way_long_with_full_fields() {
        let info = map_v2_position(v2_row(
            "BTCUSDT",
            Some("BOTH"),
            Some("0.10"),
            Some("80000"),
            Some("80100"),
            Some("10"),
            Some("0"),
            Some(1_726_400_000_123),
        ))
        .unwrap()
        .unwrap();
        assert_eq!(info.instrument, "BTCUSDT");
        assert_eq!(info.position_side, OrderPositionSide::Both);
        assert_eq!(info.signed_quantity, Decimal::from_str("0.10").unwrap());
        assert_eq!(info.entry_price, Decimal::from_str("80000").unwrap());
        assert_eq!(info.mark_price, Some(Decimal::from_str("80100").unwrap()));
        // notional = mark × |qty| = 8010。
        assert_eq!(info.notional, Some(Decimal::from_str("8010").unwrap()));
        assert_eq!(info.leverage, Some(10));
        // 交叉持仓 isolatedMargin="0" -> None（留给宿主算）。
        assert_eq!(info.margin, None);
        assert_eq!(info.update_ms, Some(1_726_400_000_123));
        assert_eq!(info.unrealized_pnl, Some(Decimal::from_str("5.5").unwrap()));
    }

    #[test]
    fn maps_short_negative_amount_and_isolated_margin() {
        let info = map_v2_position(v2_row(
            "ETHUSDT",
            Some("BOTH"),
            Some("-2"),
            Some("2000"),
            Some("1990"),
            Some("5"),
            Some("796"),
            None,
        ))
        .unwrap()
        .unwrap();
        assert_eq!(info.signed_quantity, Decimal::from_str("-2").unwrap());
        // 空仓 notional 仍为正（mark × |qty| = 3980）。
        assert_eq!(info.notional, Some(Decimal::from_str("3980").unwrap()));
        assert_eq!(info.margin, Some(Decimal::from_str("796").unwrap()));
        assert_eq!(info.update_ms, None);
    }

    #[test]
    fn hedge_long_and_short_rows_map_independently() {
        let long = map_v2_position(v2_row(
            "BTCUSDT",
            Some("LONG"),
            Some("0.1"),
            Some("80000"),
            None,
            Some("10"),
            None,
            None,
        ))
        .unwrap()
        .unwrap();
        let short = map_v2_position(v2_row(
            "BTCUSDT",
            Some("SHORT"),
            Some("-0.1"),
            Some("80000"),
            None,
            Some("10"),
            None,
            None,
        ))
        .unwrap()
        .unwrap();
        assert_eq!(long.position_side, OrderPositionSide::Long);
        assert_eq!(short.position_side, OrderPositionSide::Short);
        // 缺 markPrice 时 notional 为 None，但行仍保留（价格由宿主行情兜底）。
        assert_eq!(long.notional, None);
        assert_eq!(long.mark_price, None);
    }

    #[test]
    fn zero_quantity_and_empty_symbol_rows_are_filtered() {
        assert!(map_v2_position(v2_row(
            "BTCUSDT",
            Some("BOTH"),
            Some("0"),
            None,
            None,
            None,
            None,
            None
        ))
        .unwrap()
        .is_none());
        let mut no_symbol = v2_row("BTCUSDT", None, Some("1"), None, None, None, None, None);
        no_symbol.symbol = Some(String::new());
        assert!(map_v2_position(no_symbol).unwrap().is_none());
    }

    #[test]
    fn plus_signed_amount_parses_bad_leverage_is_none_and_bad_qty_errors() {
        let info = map_v2_position(v2_row(
            "BTCUSDT",
            None,
            Some("+0.5"),
            None,
            None,
            Some("oops"), // 非法杠杆不致命，降级为 None
            None,
            None,
        ))
        .unwrap()
        .unwrap();
        assert_eq!(info.signed_quantity, Decimal::from_str("0.5").unwrap());
        assert_eq!(info.leverage, None);

        let bad = v2_row(
            "BTCUSDT",
            None,
            Some("not-a-number"),
            None,
            None,
            None,
            None,
            None,
        );
        // 数量是关键金额字段：非法串必须上抛而非静默归 0。
        assert!(map_v2_position(bad).is_err());
    }

    /// 真实 Binance U 本位**测试网**冒烟：同步规则 → 下限价单（经本地校验对齐）
    /// → 查单 → 撤单。默认 `#[ignore]`，需显式运行：
    ///
    /// ```sh
    /// BINANCE_TESTNET_KEY=xxx BINANCE_TESTNET_SECRET=yyy \
    ///   cargo test -p trading-exchange --lib -- --ignored binance_testnet
    /// ```
    ///
    /// 挂一个远离市价的限价单，正常情况下停留在 NEW，随后立即撤销，不产生成交。
    #[tokio::test]
    #[ignore = "需要 BINANCE_TESTNET_KEY/SECRET 与外网，手动运行"]
    async fn binance_testnet_place_query_cancel_smoke() {
        let key = std::env::var("BINANCE_TESTNET_KEY").expect("BINANCE_TESTNET_KEY");
        let secret = std::env::var("BINANCE_TESTNET_SECRET").expect("BINANCE_TESTNET_SECRET");

        let table = Arc::new(OrderRuleTable::new());
        let fetcher = BinanceRuleFetcher::new(true);
        let n = fetcher.sync_rules(&table).await.expect("sync exchangeInfo");
        assert!(n > 100, "测试网应同步到大量永续标的，实际 {n}");
        assert!(table
            .get(&trading_models::OrderRuleKey::new(
                EXCHANGE,
                InstrumentType::Futures,
                "BTCUSDT"
            ))
            .is_some());

        let user = BinanceUser::with_credentials(
            &key,
            &secret,
            true,
            PositionMode::OneWay,
            Arc::clone(&table),
        )
        .expect("build user");

        // 参考价用于把限价挂到远离成交的位置：半价买单不会成交。
        let ref_price = user.get_reference_price("BTCUSDT").await.unwrap();
        let far = ref_price.price / Decimal::from(2);

        // 按该标的规则反推数量：半价处仍留 3× 最小名义额余量，再向上对齐步长，
        // 避免本地校验 NotionalBelowMin（测试网各标的名义额下限不同）。
        let row = fetcher
            .ensure_rule(&table, InstrumentType::Futures, "BTCUSDT")
            .await
            .unwrap();
        let min_notional = row.notional_min.unwrap_or(Decimal::ZERO);
        let step = row.qty_step;
        let raw = (min_notional * Decimal::from(3)) / far;
        let qty = ((raw / step).ceil() * step).max(row.qty_min.unwrap_or(step));
        assert!(
            far * qty >= min_notional,
            "测试数量名义额 {} 应 >= {min_notional}",
            far * qty
        );

        let cid = format!("0x{}", hex32());

        let req = PlaceOrderRequest {
            instrument: "BTCUSDT".into(),
            instrument_type: InstrumentType::Futures,
            side: Side::Buy,
            order_type: OrderType::Limit,
            price: Some(far),
            quantity: qty,
            reduce_only: false,
            position_side: crate::api::OrderPositionSide::Both,
            time_in_force: None,
            client_order_id: cid.clone(),
        };
        let ack = user.place_order(&req).await.expect("place order");
        assert!(ack.accepted);
        assert_eq!(ack.client_order_id, cid);
        assert!(ack.exchange_order_id.is_some(), "ACK 应返回交易所单号");

        let order_ref = OrderRef::new(cid.clone())
            .with_exchange(ack.exchange_order_id.clone().expect("exchange_order_id"));
        let state = match user.query_order("BTCUSDT", &order_ref).await {
            OrderLookup::Found(s) => s,
            other => panic!("expected Found, got {other:?}"),
        };
        assert!(matches!(
            state.status,
            OrderStatus::New | OrderStatus::Canceled
        ));

        let cancel = user
            .cancel_order("BTCUSDT", &order_ref)
            .await
            .expect("cancel");
        assert!(cancel.accepted);
    }

    #[test]
    fn place_error_bad_request_is_rejected() {
        // HTTP 400 业务拒绝（-2010 ReduceOnly 被拒）：明确 Rejected。
        let err = anyhow::Error::new(ConnectorError::BadRequestError {
            msg: "ReduceOnly Order is rejected.".into(),
            code: Some(-2010),
        });
        assert_eq!(classify_place_error_chain(&err), PlaceErrorKind::Rejected);
    }

    #[test]
    fn place_error_bad_request_nested_in_context_is_rejected() {
        // 错误被 anyhow context 包裹：沿错误链仍能识别内层 BadRequest。
        let err = anyhow::Error::new(ConnectorError::BadRequestError {
            msg: "x".into(),
            code: Some(-2010),
        })
        .context("REST 下单失败");
        assert_eq!(classify_place_error_chain(&err), PlaceErrorKind::Rejected);
    }

    #[test]
    fn place_error_network_and_timeout_are_indeterminate() {
        // 网络错误 / 非 400 错误：应答不确定，Indeterminate。
        let network = anyhow::Error::new(ConnectorError::NetworkError("connection reset".into()));
        assert_eq!(
            classify_place_error_chain(&network),
            PlaceErrorKind::Indeterminate
        );
        let server = anyhow::Error::new(ConnectorError::ConnectorClientError {
            msg: "timeout".into(),
            code: None,
        });
        assert_eq!(
            classify_place_error_chain(&server),
            PlaceErrorKind::Indeterminate
        );
    }

    #[test]
    fn place_error_generic_string_is_indeterminate() {
        // 不含 ConnectorError 的任意错误（如本地构造失败）：保守 Indeterminate。
        let err = anyhow::anyhow!("something else");
        assert_eq!(
            classify_place_error_chain(&err),
            PlaceErrorKind::Indeterminate
        );
    }

    /// 生成 32 位十六进制（非加密要求，仅满足 client_order_id 0x+32hex 形态）。
    fn hex32() -> String {
        use std::time::{SystemTime, UNIX_EPOCH};
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!("{:032x}", nanos)
    }
}
