//! ENRG-HFT — the conductor.
//!
//! Four layers, one binary:
//!
//! 1. `exchange::htx` streams HTX market data on its own task;
//! 2. `quant_core` + `strategy` watch the price window for dislocations;
//! 3. `ai_brain` polls DeepSeek for a risk verdict and feeds the execution gate;
//! 4. `execution::engine` turns a fresh `TRADE` verdict plus a signal into an
//!    order, manages the position and flattens on Ctrl-C.
//!
//! Execution defaults to *paper*: real orders require `EXECUTION_MODE=live` and
//! HTX credentials.

mod ai_brain;
mod exchange;
mod execution;
mod quant_core;
mod strategy;

use std::time::Duration;

use tokio::sync::{mpsc, watch};
use tokio::time::interval;

use crate::exchange::htx::{self, MarketEvent};
use crate::execution::engine::{
    AccountView, ExecutionCommand, ExecutionMode, Executor, ExecutorConfig,
};
use crate::strategy::{MarketSnapshot, Strategy};

const DEFAULT_AI_INTERVAL_SECS: u64 = 60;

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();
    println!(
        "[BOOT] {} {} starting",
        env!("CARGO_PKG_NAME"),
        env!("CARGO_PKG_VERSION")
    );

    if std::env::var("DEEPSEEK_API_KEY").is_err() {
        eprintln!(
            "[BOOT] DEEPSEEK_API_KEY is not set: the AI gate stays closed, so no entries will be executed"
        );
    }

    let mode = match ExecutionMode::from_env() {
        Ok(mode) => mode,
        Err(error) => {
            eprintln!("[BOOT] {error}");
            return;
        }
    };
    let config = match ExecutorConfig::from_env() {
        Ok(config) => config,
        Err(error) => {
            eprintln!("[BOOT] {error}");
            return;
        }
    };

    println!(
        "[BOOT] symbol={} execution={} order_notional={}USDT paper_cash={}USDT",
        config.symbol,
        mode.as_str(),
        config.order_notional,
        config.paper_start_cash
    );
    if mode == ExecutionMode::Live {
        println!("[BOOT] LIVE MODE: orders will be sent to HTX for real money");
    }

    // Layer 1 -> 2: the socket must never block the brain.
    let (market_tx, mut market_rx) = mpsc::channel::<MarketEvent>(htx::EVENT_BUFFER);

    // Latest market and account state, read by the executor and the AI layer.
    let (snapshot_tx, snapshot_rx) = watch::channel(MarketSnapshot::default());
    let (account_tx, account_rx) = watch::channel(AccountView::default());

    // Strategy / AI -> executor.
    let (exec_tx, exec_rx) = mpsc::channel::<ExecutionCommand>(256);

    tokio::spawn(async move {
        htx::run_websocket(market_tx).await;
    });

    // Layer 2.5: anomaly detection into a typed signal.
    let signal_tx = exec_tx.clone();
    let symbol = config.symbol.clone();
    tokio::spawn(async move {
        let mut strategy = Strategy::new(symbol);
        while let Some(event) = market_rx.recv().await {
            if let Some(signal) = strategy.on_event(&event) {
                println!(
                    "[SIGNAL] {} {} ({}%) — {}",
                    signal.side.as_str(),
                    signal.price,
                    signal.move_pct,
                    signal.reason
                );
                if signal_tx
                    .send(ExecutionCommand::Signal(signal))
                    .await
                    .is_err()
                {
                    break;
                }
            }
            snapshot_tx.send_replace(strategy.snapshot().clone());
        }
    });

    // Layer 4: the executor, or a drainer when execution is disabled.
    let mut executor_handle = None;
    if mode == ExecutionMode::Off {
        println!("[BOOT] execution is off: signals are logged only");
        tokio::spawn(drain_commands(exec_rx));
    } else {
        match Executor::new(config, mode, account_tx.clone()) {
            Ok(executor) => {
                let market = snapshot_rx.clone();
                executor_handle = Some(tokio::spawn(async move {
                    executor.run(exec_rx, market).await;
                }));
            }
            Err(error) => {
                eprintln!("[BOOT] execution engine disabled: {error}");
                tokio::spawn(drain_commands(exec_rx));
            }
        }
    }

    // Layer 3: the AI risk manager, now with real market and account context.
    let ai_commands = exec_tx.clone();
    let ai_market = snapshot_rx.clone();
    let ai_account = account_rx.clone();
    tokio::spawn(async move {
        let mut ticker = interval(ai_interval());
        loop {
            ticker.tick().await;

            let snapshot = ai_market.borrow().clone();
            if snapshot.last_price.is_none() {
                println!("[AI] waiting for the first market tick…");
                continue;
            }
            let account = ai_account.borrow().clone();
            let context = format!("{}\naccount: {}", snapshot.describe(), account.describe());
            let stamp = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S UTC");

            println!("[AI] {stamp} asking DeepSeek: {context}");
            match ai_brain::ask_deepseek(&context).await {
                Ok(decision) => {
                    println!(
                        "[AI] {stamp} decision: {} — {}",
                        decision.action.as_str(),
                        decision.reason
                    );
                    if ai_commands
                        .send(ExecutionCommand::Verdict {
                            action: decision.action,
                            reason: decision.reason,
                        })
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                Err(error) => eprintln!("[AI] {stamp} error: {error}"),
            }
        }
    });

    println!("[BOOT] running: Ctrl-C halts the bot and flattens the position");
    match tokio::signal::ctrl_c().await {
        Ok(()) => println!("[BOOT] Ctrl-C received, halting…"),
        Err(error) => eprintln!("[BOOT] could not install the Ctrl-C handler: {error}"),
    }

    let _ = exec_tx.send(ExecutionCommand::Halt("ctrl-c".into())).await;

    // Give the executor time to cancel and flatten before the runtime exits.
    if let Some(handle) = executor_handle {
        match tokio::time::timeout(Duration::from_secs(15), handle).await {
            Ok(Ok(())) => println!("[BOOT] executor stopped cleanly"),
            Ok(Err(error)) => eprintln!("[BOOT] executor task failed: {error}"),
            Err(_) => eprintln!("[BOOT] executor did not stop within 15s; exiting anyway"),
        }
    } else {
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// `AI_INTERVAL_SECS`, defaulting to 60.
fn ai_interval() -> Duration {
    let secs = std::env::var("AI_INTERVAL_SECS")
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .filter(|secs| *secs > 0)
        .unwrap_or(DEFAULT_AI_INTERVAL_SECS);
    Duration::from_secs(secs)
}

/// Consumes commands when no executor is running, so producers never block.
async fn drain_commands(mut commands: mpsc::Receiver<ExecutionCommand>) {
    while let Some(command) = commands.recv().await {
        if let ExecutionCommand::Signal(signal) = command {
            println!(
                "[SIGNAL] (execution off) {} {} — {}",
                signal.side.as_str(),
                signal.price,
                signal.reason
            );
        }
    }
}
