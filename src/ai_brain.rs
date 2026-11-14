//! Layer 3 — the brain.
//!
//! A thin, typed wrapper around the DeepSeek chat-completions API. The model is
//! only allowed to answer with a JSON `AiDecision`; anything else (including the
//! markdown fences LLMs love) is normalised before parsing.

use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::scanner::normalize_symbol;

const API_URL: &str = "https://api.deepseek.com/chat/completions";
const MODEL: &str = "deepseek-chat";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// The exact contract sent to the model.
pub const SYSTEM_PROMPT: &str = "Ты — риск-менеджер мульти-парного скальпинг-бота на HTX. Тебе дают список USDT-пар с 24ч волатильностью, изменением цены, объёмом и текущим спредом. Выбери БОЛЬШИНСТВО пар, пригодных для скальпинга (все, где движение и спред приемлемы), самые привлекательные первыми; не ограничивайся одной парой. Если пригодных нет — PAUSE с пустым массивом. Верни ТОЛЬКО валидный JSON без markdown-разметки: {\"action\": \"TRADE\" | \"PAUSE\", \"symbols\": [\"<пары в нижнем регистре>\"], \"reason\": \"краткая причина\"}";

/// What the bot is allowed to do after the model has spoken.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum AiAction {
    Trade,
    Pause,
}

impl AiAction {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Trade => "TRADE",
            Self::Pause => "PAUSE",
        }
    }
}

/// The model's answer. `reason` is intentionally free-form: it is logged, not
/// parsed. `symbols` names every pair the model cleared, most promising first;
/// the executor re-validates the list against the live scanner watchlist before
/// opening the gate. The legacy singular `symbol` field is still accepted so an
/// older prompt or a model that ignores the array contract cannot wedge the bot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AiDecision {
    pub action: AiAction,
    #[serde(default, deserialize_with = "deserialize_symbols")]
    pub symbols: Vec<String>,
    pub reason: String,
}

/// Wire shape that accepts both the new `symbols` array and the legacy singular
/// `symbol`. Parsing both at once is deliberate: a model may repeat the old
/// field next to the new one, and that must not turn a valid verdict into an
/// error.
#[derive(Deserialize)]
struct WireDecision {
    action: AiAction,
    #[serde(default, deserialize_with = "deserialize_symbols")]
    symbols: Vec<String>,
    #[serde(default, deserialize_with = "deserialize_symbols")]
    symbol: Vec<String>,
    reason: String,
}

/// Accepts `["a", "b"]`, a single `"a"`, or a comma-separated `"a, b"` and
/// treats a missing/null field as an empty list.
fn deserialize_symbols<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum RawSymbols {
        Many(Vec<String>),
        One(String),
    }

    let raw = Option::<RawSymbols>::deserialize(deserializer)?;
    Ok(match raw {
        None => Vec::new(),
        Some(RawSymbols::Many(symbols)) => symbols,
        Some(RawSymbols::One(symbols)) => symbols.split(',').map(str::to_string).collect(),
    })
}

#[derive(Debug, Deserialize)]
struct ChatResponse {
    choices: Vec<ChatChoice>,
}

#[derive(Debug, Deserialize)]
struct ChatChoice {
    message: ChatMessage,
}

#[derive(Debug, Deserialize)]
struct ChatMessage {
    content: String,
}

/// Asks DeepSeek for a risk verdict on `context`.
///
/// The API key is read from `DEEPSEEK_API_KEY` on every call so a rotated key in
/// the environment is picked up without a restart. The error type is
/// `Send + Sync` because the call is driven from a spawned task.
pub async fn ask_deepseek(
    context: &str,
) -> Result<AiDecision, Box<dyn std::error::Error + Send + Sync>> {
    let api_key = std::env::var("DEEPSEEK_API_KEY")?;
    let client = reqwest::Client::builder()
        .timeout(REQUEST_TIMEOUT)
        .build()?;

    let request = json!({
        "model": MODEL,
        "messages": [
            { "role": "system", "content": SYSTEM_PROMPT },
            { "role": "user", "content": context },
        ],
        "temperature": 0.0,
        "stream": false,
    });

    let response = client
        .post(API_URL)
        .bearer_auth(api_key)
        .json(&request)
        .send()
        .await?;

    let status = response.status();
    let body = response.text().await?;
    if !status.is_success() {
        return Err(std::io::Error::other(format!(
            "DeepSeek HTTP {status}: {}",
            truncate(&body, 400)
        ))
        .into());
    }

    let chat: ChatResponse = serde_json::from_str(&body).map_err(|err| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "DeepSeek response is not a chat completion: {err}; body={}",
                truncate(&body, 400)
            ),
        )
    })?;

    let content = chat
        .choices
        .first()
        .map(|choice| choice.message.content.as_str())
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "DeepSeek returned no choices",
            )
        })?;

    parse_decision(content).map_err(|err| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "DeepSeek returned invalid decision JSON: {err}; content={}",
                truncate(content, 400)
            ),
        )
        .into()
    })
}

/// Parses the model's text into a decision, tolerating markdown fences and
/// chatter around the JSON object. Every pair is normalised to HTX's lower-case
/// form, so `"LISTA-USDT"` and `"listausdt"` are the same market, and duplicates
/// (plus blank rows) are dropped while the model's ranking is preserved.
pub fn parse_decision(content: &str) -> Result<AiDecision, serde_json::Error> {
    let wire: WireDecision = serde_json::from_str(sanitize_json(content))?;
    let mut seen = std::collections::HashSet::new();
    let symbols = wire
        .symbols
        .iter()
        .chain(wire.symbol.iter())
        .filter_map(|symbol| normalize_symbol(symbol))
        .filter(|symbol| seen.insert(symbol.clone()))
        .collect();
    Ok(AiDecision {
        action: wire.action,
        symbols,
        reason: wire.reason,
    })
}

/// Strips ```json fences and everything before the first `{` / after the last
/// `}`.
fn sanitize_json(content: &str) -> &str {
    let trimmed = content.trim();
    let unfenced = if let Some(rest) = trimmed.strip_prefix("```") {
        let rest = rest
            .strip_prefix("json")
            .or_else(|| rest.strip_prefix("JSON"))
            .unwrap_or(rest);
        rest.strip_suffix("```").unwrap_or(rest).trim()
    } else {
        trimmed
    };

    match (unfenced.find('{'), unfenced.rfind('}')) {
        (Some(start), Some(end)) if end >= start => &unfenced[start..=end],
        _ => unfenced,
    }
}

fn truncate(value: &str, max_chars: usize) -> String {
    if value.chars().count() <= max_chars {
        return value.to_string();
    }
    let mut truncated: String = value.chars().take(max_chars).collect();
    truncated.push('…');
    truncated
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_plain_decision_with_a_pair_list() {
        let decision = parse_decision(
            r#"{"action":"TRADE","symbols":["LISTA-USDT","trumpusdt"],"reason":"pump"}"#,
        )
        .expect("plain JSON must parse");
        assert_eq!(decision.action, AiAction::Trade);
        assert_eq!(decision.symbols, vec!["listausdt", "trumpusdt"]);
        assert_eq!(decision.reason, "pump");
    }

    #[test]
    fn parses_fenced_decision() {
        let content =
            "```json\n{\"action\":\"PAUSE\",\"symbols\":[],\"reason\":\"spread too wide\"}\n```";
        let decision = parse_decision(content).expect("fenced JSON must parse");
        assert_eq!(decision.action, AiAction::Pause);
        assert!(decision.symbols.is_empty());
    }

    #[test]
    fn parses_decision_with_surrounding_text() {
        let content =
            "Sure: {\"action\": \"PAUSE\", \"reason\": \"calm\"} — no pair worth trading.";
        let decision = parse_decision(content).expect("embedded JSON must parse");
        assert_eq!(decision.action, AiAction::Pause);
        assert!(
            decision.symbols.is_empty(),
            "a missing pair list defaults to no pairs"
        );
    }

    #[test]
    fn accepts_the_legacy_single_symbol_and_comma_lists() {
        let one = parse_decision(r#"{"action":"TRADE","symbol":"LISTA-USDT","reason":"old"}"#)
            .expect("legacy JSON must parse");
        assert_eq!(one.symbols, vec!["listausdt"]);

        let many =
            parse_decision(r#"{"action":"TRADE","symbol":"LISTA-USDT, TRUMPUSDT","reason":"old"}"#)
                .expect("comma-separated JSON must parse");
        assert_eq!(many.symbols, vec!["listausdt", "trumpusdt"]);

        let both = parse_decision(
            r#"{"action":"TRADE","symbols":["BTCUSDT"],"symbol":"LISTA-USDT","reason":"old"}"#,
        )
        .expect("both fields must parse");
        assert_eq!(both.symbols, vec!["btcusdt", "listausdt"]);
    }

    #[test]
    fn normalises_and_deduplicates_pairs_in_order() {
        let decision = parse_decision(
            r#"{"action":"TRADE","symbols":["TRUMP-USDT","listausdt","trumpusdt","","  "],"reason":"ranked"}"#,
        )
        .expect("duplicates must be tolerated");
        assert_eq!(
            decision.symbols,
            vec!["trumpusdt", "listausdt"],
            "blank rows and repeats are dropped without reordering"
        );
    }

    #[test]
    fn rejects_unknown_action() {
        assert!(parse_decision(r#"{"action":"BUY","symbol":"btcusdt","reason":"oops"}"#).is_err());
    }

    #[test]
    fn serialises_action_in_uppercase() {
        let decision = AiDecision {
            action: AiAction::Trade,
            symbols: vec!["listausdt".into(), "trumpusdt".into()],
            reason: "test".to_string(),
        };
        let json = serde_json::to_string(&decision).expect("decision must serialise");
        assert!(json.contains("\"action\":\"TRADE\""));
        assert!(json.contains("\"symbols\":[\"listausdt\",\"trumpusdt\"]"));
    }
}
