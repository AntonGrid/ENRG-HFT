//! Layer 3 — the brain.
//!
//! A thin, typed wrapper around the DeepSeek chat-completions API. The model is
//! only allowed to answer with a JSON `AiDecision`; anything else (including the
//! markdown fences LLMs love) is normalised before parsing.

use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::json;

const API_URL: &str = "https://api.deepseek.com/chat/completions";
const MODEL: &str = "deepseek-chat";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// The exact contract sent to the model.
pub const SYSTEM_PROMPT: &str = "Ты — риск-менеджер скальпинг-бота. Оцени контекст рынка и верни ТОЛЬКО валидный JSON без markdown-разметки: {\"action\": \"TRADE\" | \"PAUSE\", \"reason\": \"краткая причина\"}";

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
/// parsed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AiDecision {
    pub action: AiAction,
    pub reason: String,
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
/// chatter around the JSON object.
pub fn parse_decision(content: &str) -> Result<AiDecision, serde_json::Error> {
    serde_json::from_str(sanitize_json(content))
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
    fn parses_plain_decision() {
        let decision = parse_decision(r#"{"action":"TRADE","reason":"volatility spike"}"#)
            .expect("plain JSON must parse");
        assert_eq!(decision.action, AiAction::Trade);
        assert_eq!(decision.reason, "volatility spike");
    }

    #[test]
    fn parses_fenced_decision() {
        let content = "```json\n{\"action\":\"PAUSE\",\"reason\":\"spread too wide\"}\n```";
        let decision = parse_decision(content).expect("fenced JSON must parse");
        assert_eq!(decision.action, AiAction::Pause);
    }

    #[test]
    fn parses_decision_with_surrounding_text() {
        let content = "Sure, here it is: {\"action\": \"PAUSE\", \"reason\": \"calm\"} — done.";
        let decision = parse_decision(content).expect("embedded JSON must parse");
        assert_eq!(decision.action, AiAction::Pause);
    }

    #[test]
    fn rejects_unknown_action() {
        assert!(parse_decision(r#"{"action":"BUY","reason":"oops"}"#).is_err());
    }

    #[test]
    fn serialises_action_in_uppercase() {
        let decision = AiDecision {
            action: AiAction::Trade,
            reason: "test".to_string(),
        };
        let json = serde_json::to_string(&decision).expect("decision must serialise");
        assert!(json.contains("\"action\":\"TRADE\""));
    }
}
