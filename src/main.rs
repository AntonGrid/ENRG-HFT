//! ENRG-HFT — the conductor.
//!
//! Layer 1 streams HTX market data on its own task, layer 2 watches the price
//! window for dislocations, and layer 3 (DeepSeek) is polled once a minute for a
//! risk verdict. Nothing is traded yet: this sprint is the skeleton.

mod ai_brain;
mod exchange;
mod quant_core;

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use rust_decimal::Decimal;
use tokio::sync::mpsc;
use tokio::time::interval;

use crate::exchange::htx::{self, MarketEvent};

const AI_INTERVAL: Duration = Duration::from_secs(60);
const ANOMALY_COOLDOWN: Duration = Duration::from_secs(5);

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();
    println!(
        "[BOOT] {} {} starting",
        env!("CARGO_PKG_NAME"),
        env!("CARGO_PKG_VERSION")
    );

    if std::env::var("DEEPSEEK_API_KEY").is_err() {
        eprintln!("[BOOT] DEEPSEEK_API_KEY is not set: the AI layer will log errors");
    }

    let (tx, mut rx) = mpsc::channel::<MarketEvent>(htx::EVENT_BUFFER);

    // Layer 1: the socket must never block the brain.
    tokio::spawn(async move {
        htx::run_websocket(tx).await;
    });

    // Layer 2: consume ticks and look for >1% dislocations in the last 10 ticks.
    tokio::spawn(async move {
        let mut window: VecDeque<Decimal> =
            VecDeque::with_capacity(quant_core::ANOMALY_WINDOW_TICKS);
        let mut last_anomaly: Option<Instant> = None;

        while let Some(event) = rx.recv().await {
            let MarketEvent::Trade {
                price, received_ns, ..
            } = event
            else {
                continue;
            };

            if window.len() == quant_core::ANOMALY_WINDOW_TICKS {
                window.pop_front();
            }
            window.push_back(price);

            let snapshot: Vec<Decimal> = window.iter().copied().collect();
            if quant_core::detect_anomaly(&snapshot)
                && last_anomaly.map_or(true, |at| at.elapsed() >= ANOMALY_COOLDOWN)
            {
                let low = snapshot.iter().copied().min().unwrap_or(price);
                let high = snapshot.iter().copied().max().unwrap_or(price);
                println!(
                    "[ANOMALY] >{}% move in last {} ticks: low={low} high={high} last={price} time={received_ns}",
                    quant_core::ANOMALY_THRESHOLD_PCT,
                    snapshot.len()
                );
                last_anomaly = Some(Instant::now());
            }
        }
    });

    // Layer 3: test call once a minute. `interval` fires immediately, so the
    // first verdict arrives as soon as the bot boots.
    let mut ticker = interval(AI_INTERVAL);
    loop {
        ticker.tick().await;
        let stamp = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S UTC");
        println!("[AI] {stamp} asking DeepSeek (context: \"Price stable\")…");

        match ai_brain::ask_deepseek("Price stable").await {
            Ok(decision) => println!(
                "[AI] {stamp} decision: {} — {}",
                decision.action.as_str(),
                decision.reason
            ),
            Err(err) => eprintln!("[AI] {stamp} error: {err}"),
        }
    }
}
