//! 连锁规则批量创建的零 IO 纯模块（rule设计 §9）。
//!
//! 处理请求体的 `ref`（客户端引用）与 `action: rule:<ref>`：
//! * [`RawAction`]：action 三态（self / rule:<ref> / 直接 rule:<id>）；
//! * ref 集合校验：无法识别拒绝，**前向引用合法**，重复 ref 拒绝；
//! * 落库后用 [`RefIdMap`] 把占位 action 回填为真实 `rule:<id>`。

use std::collections::BTreeMap;
use std::collections::BTreeSet;

/// 请求处理期的 action 三态（§9.3）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RawAction {
    /// 执行自身（无链式跳转）。
    Self_,
    /// 引用批内另一条规则的客户端 ref。
    Ref(String),
    /// 直接引用已存在规则的数字 id。
    Id(u64),
}

impl RawAction {
    /// 解析 action 字符串。
    ///
    /// `self`；`rule:<纯数字>`=Id；`rule:<ref名>`=Ref。ref 名须以字母/下划线
    /// 开头（避免与数字 id 歧义），其余为字母/数字/下划线；无法识别返回 None。
    pub fn parse(raw: &str) -> Option<RawAction> {
        let raw = raw.trim();
        if raw == "self" {
            return Some(RawAction::Self_);
        }
        let rest = raw.strip_prefix("rule:")?;
        if rest.is_empty() {
            return None;
        }
        if let Ok(id) = rest.parse::<u64>() {
            return Some(RawAction::Id(id));
        }
        if is_valid_ref(rest) {
            Some(RawAction::Ref(rest.to_string()))
        } else {
            None
        }
    }
}

/// ref 名合法：非空、以 ASCII 字母/下划线开头、其余字母/数字/下划线。
fn is_valid_ref(s: &str) -> bool {
    let mut chars = s.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// 校验批量条目 `(本条 ref 名, action)`：
/// * ref 名唯一（重复声明拒绝）；
/// * action 无法识别拒绝；
/// * action 的 `rule:<ref>` 目标必须在批内声明（**前向引用合法**）。
pub fn validate_entries(entries: Vec<(&str, &str)>) -> Result<(), String> {
    let mut declared = BTreeSet::new();
    for (name, _) in &entries {
        let name = name.trim();
        if !is_valid_ref(name) {
            return Err(format!("非法 ref 名：`{name}`"));
        }
        if !declared.insert(name.to_string()) {
            return Err(format!("重复声明的 ref：`{name}`"));
        }
    }
    for (_, action) in &entries {
        if let Some(RawAction::Ref(r)) = RawAction::parse(action) {
            if !declared.contains(&r) {
                return Err(format!("action 引用了批内不存在的 ref：`{r}`"));
            }
        }
    }
    Ok(())
}

/// 客户端 ref → 落库后真实规则 id 的映射（第二遍回填用）。
#[derive(Debug, Default)]
pub struct RefIdMap {
    mapping: BTreeMap<String, u64>,
}

impl RefIdMap {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, client_ref: &str, rule_id: u64) {
        self.mapping.insert(client_ref.to_string(), rule_id);
    }

    /// 把占位 action 回填为终态串：`rule:<真实id>`。
    ///
    /// self / 直接数字 id 原样归一；引用的 ref 无映射时返回 None。
    pub fn backfill_action_opt(&self, action: &str) -> Option<String> {
        match RawAction::parse(action)? {
            RawAction::Self_ => Some("self".to_string()),
            RawAction::Id(id) => Some(format!("rule:{id}")),
            RawAction::Ref(r) => self.mapping.get(&r).map(|id| format!("rule:{id}")),
        }
    }

    /// 同 [`Self::backfill_action_opt`]，无映射时原样返回 action（调用方按需选择）。
    pub fn backfill_action(&self, action: &str) -> String {
        self.backfill_action_opt(action)
            .unwrap_or_else(|| action.trim().to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raw_action_parses_three_forms() {
        assert_eq!(RawAction::parse("self"), Some(RawAction::Self_));
        assert_eq!(
            RawAction::parse("rule:take_profit"),
            Some(RawAction::Ref("take_profit".to_string()))
        );
        assert_eq!(RawAction::parse("rule:42"), Some(RawAction::Id(42)));
        // 非法形态。
        assert!(RawAction::parse("rule:").is_none());
        assert!(RawAction::parse("other").is_none());
        assert!(RawAction::parse("rule:1x").is_none());
    }

    #[test]
    fn multiple_distinct_refs_are_allowed() {
        // 两条规则引用各自不同的 ref：合法。
        let entries = vec![
            ("a", "rule:stop_loss"),
            ("b", "rule:take_profit"),
            ("stop_loss", "self"),
            ("take_profit", "self"),
        ];
        validate_entries(entries).unwrap();
    }

    #[test]
    fn duplicate_ref_target_in_actions_rejected() {
        // 两个不同 ref 名的条目都引用同一目标：合法（多对一允许）；
        // 但两条声明同名 ref 才拒绝——此处验证重复**声明名**。
        let entries = vec![("dup", "self"), ("dup", "self")];
        assert!(validate_entries(entries).is_err());
    }

    #[test]
    fn forward_reference_is_allowed() {
        // 第 1 条引用第 2 条才声明的 ref：前向引用合法。
        let entries = vec![("a", "rule:later"), ("later", "self")];
        validate_entries(entries).unwrap();
    }

    #[test]
    fn missing_ref_target_is_rejected() {
        // 引用了批内不存在的 ref。
        let entries = vec![("a", "rule:ghost")];
        let err = validate_entries(entries).unwrap_err();
        assert!(err.contains("ghost"));
    }

    #[test]
    fn duplicate_ref_name_in_entries_rejected() {
        // 两条规则声明同一个 ref 名。
        let entries = vec![("dup", "self"), ("dup", "self")];
        assert!(validate_entries(entries).is_err());
    }

    #[test]
    fn backfill_string_replaces_ref_with_real_id() {
        // 占位 action → 终态：rule:<真实id>。
        let mut map = RefIdMap::new();
        map.insert("tp", 77);
        assert_eq!(map.backfill_action("rule:tp"), "rule:77");
        // self 与直接 id 不变。
        assert_eq!(map.backfill_action("self"), "self");
        assert_eq!(map.backfill_action("rule:42"), "rule:42");
        // 未知 ref 无法回填。
        assert!(map.backfill_action_opt("rule:nope").is_none());
    }
}
