//! ENRG-HFT — the conductor.
//!
//! One binary, one pipeline:
//!
//! 1. `scanner` polls the HTX ticker board, drops illiquid pairs and ranks the
//!    rest by 24h amplitude;
//! 2. `exchange::htx` streams trades and top-of-book for **all** selected pairs
//!    over a single WebSocket, adding and removing subscriptions live;
//! 3. `strategy` keeps an isolated book per symbol and emits a `Signal` tagged
//!    with the pair that produced it;
//! 4. `ai_brain` asks DeepSeek, once a minute, which pair to trade (or PAUSE);
//! 5. `execution::engine` turns a fresh per-symbol `TRADE` verdict plus a signal
//!    into an order on that exact pair, manages the position and flattens on
//!    Ctrl-C.
//!
//! Execution defaults to *paper*: real orders require `EXECUTION_MODE=live` and
//! HTX credentials. There is no default trading pair — the scanner decides.

mod ai_brain;
mod exchange;
mod execution;
mod quant_core;
mod scanner;
mod strategy;

use std::sync::{Arc, RwLock};
use std::time::Duration;

use tokio::sync::{mpsc, watch};
use tokio::time::interval;

use crate::exchange::htx::{self, MarketEvent};
use crate::execution::engine::{
    AccountView, ExecutionCommand, ExecutionMode, Executor, ExecutorConfig,
};
use crate::scanner::{PairInfo, Scanner, ScannerConfig};
use crate::strategy::{MarketBoard, Signal};

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
    let scan_config = match ScannerConfig::from_env() {
        Ok(config) => config,
        Err(error) => {
            eprintln!("[BOOT] {error}");
            return;
        }
    };
    let scanner = match Scanner::new(scan_config.clone()) {
        Ok(scanner) => scanner,
        Err(error) => {
            eprintln!("[BOOT] {error}");
            return;
        }
    };

    println!(
        "[BOOT] scanner: every {}s, min 24h volume {}USDT, top {} pairs",
        scan_config.interval.as_secs(),
        scan_config.min_quote_volume,
        scan_config.top_n
    );
    if !scan_config.pinned.is_empty() {
        println!(
            "[BOOT] scanner: pinned pairs {:?}",
            scan_config
                .pinned
                .iter()
                .map(|symbol| symbol.to_uppercase())
                .collect::<Vec<_>>()
        );
    }
    println!(
        "[BOOT] execution={} order_notional={}USDT paper_cash={}USDT",
        mode.as_str(),
        config.order_notional,
        config.paper_start_cash
    );
    if mode == ExecutionMode::Live {
        println!("[BOOT] LIVE MODE: orders will be sent to HTX for real money");
    }

    let board = Arc::new(RwLock::new(MarketBoard::new()));

    // The first scan happens before the socket: the watchlist decides what to
    // subscribe to. A failed scan is not fatal — the rescan loop retries.
    match scanner.scan().await {
        Ok(pairs) => {
            log_scan(&pairs);
            board
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .set_pairs(pairs);
        }
        Err(error) => eprintln!("[SCAN] initial scan failed: {error}; will retry"),
    }

    // Layer 1 -> 2: the socket must never block the brain.
    let (market_tx, mut market_rx) = mpsc::channel::<MarketEvent>(htx::EVENT_BUFFER);

    // Scanner -> WebSocket: the live subscription list.
    let (symbols_tx, symbols_rx) = watch::channel(
        board
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .subscriptions(),
    );

    // Latest account state, read by the AI layer.
    let (account_tx, account_rx) = watch::channel(AccountView::default());

    // Strategy / AI -> executor.
    let (exec_tx, exec_rx) = mpsc::channel::<ExecutionCommand>(256);

    tokio::spawn(async move {
        htx::run_market_stream(market_tx, symbols_rx).await;
    });

    // Layer 2.5: per-symbol anomaly detection into typed signals.
    let signal_tx = exec_tx.clone();
    let strategy_board = board.clone();
    tokio::spawn(async move {
        while let Some(event) = market_rx.recv().await {
            let signal = {
                let mut board = strategy_board
                    .write()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                board.apply(&event)
            };
            if let Some(signal) = signal {
                log_signal(&signal);
                if signal_tx
                    .send(ExecutionCommand::Signal(signal))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        }
    });

    // Layer 4: the executor, or a drainer when execution is disabled.
    let mut executor_handle = None;
    if mode == ExecutionMode::Off {
        println!("[BOOT] execution is off: signals are logged only");
        tokio::spawn(drain_commands(exec_rx));
    } else {
        match Executor::new(config, mode, board.clone(), account_tx.clone()) {
            Ok(executor) => {
                executor_handle = Some(tokio::spawn(executor.run(exec_rx)));
            }
            Err(error) => {
                eprintln!("[BOOT] execution engine disabled: {error}");
                tokio::spawn(drain_commands(exec_rx));
            }
        }
    }

    // Layer 0.5, periodic: re-rank the market and re-point the stream.
    let rescan_board = board.clone();
    let rescan_symbols = symbols_tx.clone();
    let scan_interval = scan_config.interval;
    tokio::spawn(async move {
        let mut ticker = interval(scan_interval);
        ticker.tick().await; // the initial scan already happened
        loop {
            ticker.tick().await;
            match scanner.scan().await {
                Ok(pairs) => {
                    log_scan(&pairs);
                    let (before, after) = {
                        let mut board = rescan_board
                            .write()
                            .unwrap_or_else(|poisoned| poisoned.into_inner());
                        let before = board.subscriptions();
                        board.set_pairs(pairs);
                        let after = board.subscriptions();
                        (before, after)
                    };
                    log_watchlist_changes(&before, &after);
                    rescan_symbols.send_replace(after);
                }
                Err(error) => {
                    eprintln!("[SCAN] rescan failed: {error}; keeping the current watchlist")
                }
            }
        }
    });

    // Layer 3: the AI risk manager, now choosing among all scanned pairs.
    let ai_commands = exec_tx.clone();
    let ai_board = board.clone();
    let ai_account = account_rx.clone();
    tokio::spawn(async move {
        let interval = ai_interval();
        let mut ticker = tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
        loop {
            ticker.tick().await;

            let (context, pair_count, ready) = {
                let board = ai_board
                    .read()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if board.is_empty() {
                    (String::new(), 0, false)
                } else {
                    // Do not ask the model before at least one live book exists:
                    // a watchlist full of `bid=- ask=-` only produces a PAUSE.
                    let ready = board.pairs().iter().any(|pair| {
                        board.snapshot(&pair.symbol).is_some_and(|snapshot| {
                            snapshot.best_bid.is_some() && snapshot.best_ask.is_some()
                        })
                    });
                    let account = ai_account.borrow().clone();
                    (
                        format!(
                            "watchlist:\n{}\naccount: {}",
                            board.describe_pairs(),
                            account.describe()
                        ),
                        board.pairs().len(),
                        ready,
                    )
                }
            };

            if pair_count == 0 {
                println!("[AI] waiting for the scanner watchlist…");
                continue;
            }
            if !ready {
                println!("[AI] waiting for the first top-of-book snapshot…");
                continue;
            }
            let stamp = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S UTC");

            println!("[AI] {stamp} asking DeepSeek about {pair_count} pairs:\n{context}");
            match ai_brain::ask_deepseek(&context).await {
                Ok(decision) => {
                    let symbol = decision
                        .symbol
                        .as_deref()
                        .map(|symbol| format!(" {}", symbol.to_uppercase()))
                        .unwrap_or_default();
                    println!(
                        "[AI] {stamp} decision: {}{symbol} — {}",
                        decision.action.as_str(),
                        decision.reason
                    );
                    if ai_commands
                        .send(ExecutionCommand::Verdict {
                            action: decision.action,
                            symbol: decision.symbol,
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

fn log_scan(pairs: &[PairInfo]) {
    if pairs.is_empty() {
        println!("[SCAN] no liquid USDT pair passed the filter");
        return;
    }
    println!(
        "[SCAN] found {} active pairs (ranked by 24h amplitude):",
        pairs.len()
    );
    for (index, pair) in pairs.iter().enumerate() {
        println!("[SCAN] {:>2}. {}", index + 1, pair.describe());
    }
}

fn log_watchlist_changes(before: &[String], after: &[String]) {
    let added: Vec<String> = after
        .iter()
        .filter(|symbol| !before.contains(symbol))
        .map(|symbol| symbol.to_uppercase())
        .collect();
    let removed: Vec<String> = before
        .iter()
        .filter(|symbol| !after.contains(symbol))
        .map(|symbol| symbol.to_uppercase())
        .collect();

    if !added.is_empty() {
        println!("[SCAN] added to the stream: {}", added.join(", "));
    }
    if !removed.is_empty() {
        println!("[SCAN] removed from the stream: {}", removed.join(", "));
    }
}

fn log_signal(signal: &Signal) {
    println!(
        "[SIGNAL] {} {} {} ({}%) — {}",
        signal.symbol.to_uppercase(),
        signal.side.as_str(),
        signal.price,
        signal.move_pct,
        signal.reason
    );
}

/// Consumes commands when no executor is running, so producers never block.
async fn drain_commands(mut commands: mpsc::Receiver<ExecutionCommand>) {
    while let Some(command) = commands.recv().await {
        if let ExecutionCommand::Signal(signal) = command {
            println!(
                "[SIGNAL] (execution off) {} {} {} — {}",
                signal.symbol.to_uppercase(),
                signal.side.as_str(),
                signal.price,
                signal.reason
            );
        }
    }
}
