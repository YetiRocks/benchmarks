//! WebSocket Throughput Test
//!
//! Measures real-time message delivery rate via WebSocket.
//! Writes records to ThroughputTest and measures how fast WS clients receive messages.
//!
//! Usage:
//!   cargo run --release --bin load-ws

use load_common::{
    APP_PATH, BASE_URL, TEST_DURATION_SECS, HarnessSampler, TestResult,
    env_duration, env_vus, signal_ready,
};
use reqwest::Client;
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::JoinSet;
use tokio_tungstenite::tungstenite::Message;
use futures_util::{SinkExt, StreamExt};

const TABLE: &str = "ThroughputTest";
const WS_CLIENTS: usize = 5;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let duration = env_duration(TEST_DURATION_SECS);
    let total_vus = env_vus(25);
    let write_concurrency = total_vus.saturating_sub(WS_CLIENTS).max(1);

    eprintln!("=== WebSocket Throughput Test ===");
    eprintln!("WS clients: {WS_CLIENTS}");
    eprintln!("Write concurrency: {write_concurrency}");
    eprintln!("Duration: {duration}s\n");

    let client = Client::builder()
        .danger_accept_invalid_certs(true)
        .build()?;

    let sampler = HarnessSampler::start();
    let messages_received = Arc::new(AtomicUsize::new(0));
    let writes_sent = Arc::new(AtomicUsize::new(0));
    let write_errors = Arc::new(AtomicUsize::new(0));

    let start = Instant::now();
    let deadline = start + Duration::from_secs(duration);

    let mut tasks = JoinSet::new();

    // Spawn WebSocket listener clients
    let ws_url = format!("wss://localhost:9996{}/{}/", APP_PATH, TABLE);
    for _ in 0..WS_CLIENTS {
        let messages = messages_received.clone();
        let url = ws_url.clone();

        tasks.spawn(async move {
            // Build TLS connector that accepts self-signed certs
            let connector = tokio_tungstenite::Connector::NativeTls(
                native_tls::TlsConnector::builder()
                    .danger_accept_invalid_certs(true)
                    .build()
                    .unwrap()
            );

            let (ws_stream, _) = match tokio_tungstenite::connect_async_tls_with_config(
                &url,
                None,
                false,
                Some(connector),
            ).await {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("WS connect failed: {}", e);
                    return;
                }
            };

            let (mut write, mut read) = ws_stream.split();

            while Instant::now() < deadline {
                match tokio::time::timeout(Duration::from_secs(1), read.next()).await {
                    Ok(Some(Ok(Message::Text(_)))) => {
                        messages.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(Some(Ok(_))) => {} // ping/pong/binary
                    Ok(Some(Err(e))) => {
                        eprintln!("WS error: {}", e);
                        break;
                    }
                    Ok(None) => break,
                    Err(_) => continue, // timeout
                }
            }

            // Close WebSocket with timeout — server may have a large send backlog
            let _ = tokio::time::timeout(Duration::from_secs(2), write.close()).await;
        });
    }

    // Give WS clients a moment to connect
    tokio::time::sleep(Duration::from_millis(500)).await;
    signal_ready();

    // Spawn writer tasks to generate events
    for task_id in 0..write_concurrency {
        let client = client.clone();
        let writes = writes_sent.clone();
        let errors = write_errors.clone();

        tasks.spawn(async move {
            let mut i = 0u64;
            while Instant::now() < deadline {
                let url = format!("{BASE_URL}{APP_PATH}/{TABLE}");
                let record = json!({
                    "id": format!("ws_{}_{}", task_id, i),
                    "name": format!("WS event {i}"),
                    "value": i
                });

                match client.post(&url).json(&record).send().await {
                    Ok(resp) if resp.status().is_success() => {
                        writes.fetch_add(1, Ordering::Relaxed);
                    }
                    _ => {
                        errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
                i += 1;
            }
        });
    }

    // Wait for all tasks
    while tasks.join_next().await.is_some() {}

    let elapsed = start.elapsed();
    let total_messages = messages_received.load(Ordering::Relaxed);
    let total_writes = writes_sent.load(Ordering::Relaxed);
    let err_count = write_errors.load(Ordering::Relaxed);
    let msgs_per_sec = total_messages as f64 / elapsed.as_secs_f64();
    let writes_per_sec = total_writes as f64 / elapsed.as_secs_f64();

    let estimate = sampler.finish(writes_per_sec);

    eprintln!("\n  WEBSOCKET RESULTS:");
    eprintln!("  Messages received: {total_messages}");
    eprintln!("  Writes sent: {total_writes}");
    eprintln!("  Write errors: {err_count}");
    eprintln!("  Duration: {:.2}s", elapsed.as_secs_f64());
    eprintln!("  Messages/sec: {:.0}", msgs_per_sec);
    eprintln!("  Writes/sec: {:.0}", writes_per_sec);
    eprintln!("  Harness CPU: {:.1}% of system", estimate.harness_cpu_fraction * 100.0);

    let result = TestResult {
        test: "ws".to_string(),
        throughput: writes_per_sec,
        p50: 0.0,
        p95: 0.0,
        p99: 0.0,
        total: total_writes,
        errors: err_count,
        duration_secs: elapsed.as_secs_f64(),
        harness_overhead: estimate.harness_cpu_fraction,
        extrapolated_throughput: format!("{:.0}", estimate.extrapolated_throughput),
        summary: format!("{:.0} writes/sec, {:.0} msgs/sec ({} WS clients)", writes_per_sec, msgs_per_sec, WS_CLIENTS),
    };

    result.emit();
    result.post_to_test_run().await;

    // No cleanup needed — runner truncates ThroughputTest before each test

    Ok(())
}
