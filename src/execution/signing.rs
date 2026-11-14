//! HTX (Huobi) API v1 request signing.
//!
//! The scheme is documented as:
//!
//! ```text
//! StringToSign = METHOD + "\n" + host + "\n" + path + "\n" + encoded_params
//! Signature    = Base64(HMAC-SHA256(secret_key, StringToSign))
//! ```
//!
//! where `encoded_params` are the request parameters sorted by key, each key and
//! value percent-encoded the way the official SDKs do it (`urllib.urlencode` in
//! Python, i.e. spaces become `+`). The signature itself is never part of the
//! signed payload.
//!
//! Everything here is pure and synchronous so the wire format can be pinned down
//! with golden vectors instead of hoping.

use std::collections::BTreeMap;

use base64::engine::general_purpose::STANDARD;
use base64::Engine as _;
use hmac::{Hmac, Mac};
use sha2::Sha256;

pub const SIGNATURE_METHOD: &str = "HmacSHA256";
pub const SIGNATURE_VERSION: &str = "2";

const HEX: &[u8; 16] = b"0123456789ABCDEF";

type HmacSha256 = Hmac<Sha256>;

/// Percent-encodes a single key or value.
///
/// Unreserved characters (`A-Z a-z 0-9 - _ . ~`) pass through, a space becomes
/// `+` (form encoding, matching the reference SDKs), and every other byte is
/// `%XX` with upper-case hex.
pub fn encode(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(byte as char);
            }
            b' ' => encoded.push('+'),
            _ => {
                encoded.push('%');
                encoded.push(HEX[(byte >> 4) as usize] as char);
                encoded.push(HEX[(byte & 0x0f) as usize] as char);
            }
        }
    }
    encoded
}

/// `k1=v1&k2=v2` with keys in ASCII order.
pub fn canonical_query(params: &BTreeMap<String, String>) -> String {
    params
        .iter()
        .map(|(key, value)| format!("{}={}", encode(key), encode(value)))
        .collect::<Vec<_>>()
        .join("&")
}

/// The exact text that gets hashed.
pub fn string_to_sign(
    method: &str,
    host: &str,
    path: &str,
    params: &BTreeMap<String, String>,
) -> String {
    format!(
        "{}\n{}\n{}\n{}",
        method.to_ascii_uppercase(),
        host,
        path,
        canonical_query(params)
    )
}

/// `Base64(HMAC-SHA256(secret, payload))`.
pub fn hmac_sha256_base64(secret: &str, payload: &str) -> String {
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC accepts keys of every length");
    mac.update(payload.as_bytes());
    STANDARD.encode(mac.finalize().into_bytes())
}

/// Signs a request. `params` must contain every parameter that will be sent
/// except `Signature`.
pub fn sign(
    secret: &str,
    method: &str,
    host: &str,
    path: &str,
    params: &BTreeMap<String, String>,
) -> String {
    hmac_sha256_base64(secret, &string_to_sign(method, host, path, params))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect()
    }

    #[test]
    fn hmac_matches_rfc4231_vector() {
        // RFC 4231, test case 2.
        let signature = hmac_sha256_base64("Jefe", "what do ya want for nothing?");
        assert_eq!(signature, "W9zBRr9gdU5qBCQmCJV1x1oAPwidJzmDnexYuWTsOEM=");
    }

    #[test]
    fn encodes_reserved_and_unreserved_characters() {
        assert_eq!(encode("btcusdt"), "btcusdt");
        assert_eq!(encode("2026-11-14T12:00:00"), "2026-11-14T12%3A00%3A00");
        assert_eq!(encode("a b"), "a+b");
        assert_eq!(encode("~-._"), "~-._");
        assert_eq!(encode("100%"), "100%25");
        assert_eq!(encode("+"), "%2B");
    }

    #[test]
    fn canonical_query_is_sorted_and_encoded() {
        let query = canonical_query(&params(&[
            ("Timestamp", "2026-11-14T12:00:00"),
            ("Amount", "0.001"),
            ("Symbol", "btcusdt"),
        ]));
        assert_eq!(
            query,
            "Amount=0.001&Symbol=btcusdt&Timestamp=2026-11-14T12%3A00%3A00"
        );
    }

    #[test]
    fn string_to_sign_has_four_lines() {
        let payload = string_to_sign(
            "post",
            "api.htx.com",
            "/v1/order/orders/place",
            &params(&[("Amount", "0.001")]),
        );
        assert_eq!(
            payload,
            "POST\napi.htx.com\n/v1/order/orders/place\nAmount=0.001"
        );
    }

    #[test]
    fn signature_matches_independent_golden_vector() {
        // Computed with Python's hmac/hashlib/urllib.parse (see README).
        let secret = "7a5d3a13-a1c4-4387-9d9b-d1b6a5a6e3f2";
        let signed = params(&[
            ("AccessKeyId", "abc"),
            ("Amount", "0.001"),
            ("Price", "65000"),
            ("SignatureMethod", "HmacSHA256"),
            ("SignatureVersion", "2"),
            ("Symbol", "btcusdt"),
            ("Timestamp", "2026-11-14T12:00:00"),
            ("Type", "buy-limit"),
        ]);

        assert_eq!(
            sign(
                secret,
                "POST",
                "api.htx.com",
                "/v1/order/orders/place",
                &signed
            ),
            "aLlOh7AhGCInYS8gifetJbYvjDs3oRTydNd08kbre4w="
        );
    }

    #[test]
    fn signature_depends_on_every_signed_field() {
        let secret = "secret";
        let base = params(&[("Amount", "0.001"), ("Symbol", "btcusdt")]);
        let changed = params(&[("Amount", "0.002"), ("Symbol", "btcusdt")]);

        let first = sign(
            secret,
            "POST",
            "api.htx.com",
            "/v1/order/orders/place",
            &base,
        );
        let second = sign(
            secret,
            "POST",
            "api.htx.com",
            "/v1/order/orders/place",
            &changed,
        );
        assert_ne!(first, second);
    }
}
