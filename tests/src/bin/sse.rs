//! SSE (Server-Sent Events) Throughput Test
//!
//! Measures real-time event delivery rate via SSE.
//! Writes records to ThroughputTest and measures how fast SSE clients receive events.
//!
//! Usage:
//!   cargo run --release --bin load-sse

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

const TABLE: &str = "ThroughputTest";
const SSE_CLIENTS: usize = 5;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let duration = env_duration(TEST_DURATION_SECS);
    let total_vus = env_vus(25);
    let write_concurrency = total_vus.saturating_sub(SSE_CLIENTS).max(1);

    eprintln!("=== SSE Throughput Test ===");
    eprintln!("SSE clients: {SSE_CLIENTS}");
    eprintln!("Write concurrency: {write_concurrency}");
    eprintln!("Duration: {duration}s\n");

    let client = Client::builder()
        .danger_accept_invalid_certs(true)
        .build()?;

    let sampler = HarnessSampler::start();
    let events_received = Arc::new(AtomicUsize::new(0));
    let writes_sent = Arc::new(AtomicUsize::new(0));
    let write_errors = Arc::new(AtomicUsize::new(0));

    let start = Instant::now();
    let deadline = start + Duration::from_secs(duration);

    let mut tasks = JoinSet::new();

    // Spawn SSE listener clients
    for _ in 0..SSE_CLIENTS {
        let client = client.clone();
        let events = events_received.clone();

        tasks.spawn(async move {
            let url = format!("{BASE_URL}{APP_PATH}/{TABLE}?stream=sse");
            let resp = match client.get(&url)
                .header("Accept", "text/event-stream")
                .send()
                .await
            {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("SSE connect failed: {}", e);
                    return;
                }
            };

            let mut stream = resp.bytes_stream();
            use futures_util::StreamExt;

            while Instant::now() < deadline {
                match tokio::time::timeout(Duration::from_secs(1), stream.next()).await {
                    Ok(Some(Ok(chunk))) => {
                        // Count SSE events in chunk (each event starts with "data:")
                        let text = String::from_utf8_lossy(&chunk);
                        let event_count = text.matches("data:").count();
                        events.fetch_add(event_count, Ordering::Relaxed);
                    }
                    Ok(Some(Err(e))) => {
                        eprintln!("SSE stream error: {}", e);
                        break;
                    }
                    Ok(None) => break,
                    Err(_) => continue, // timeout, check deadline
                }
            }
        });
    }

    // Give SSE clients a moment to connect
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
                    "id": format!("sse_{}_{}", task_id, i),
                    "name": format!("SSE event {i}"),
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
    let total_events = events_received.load(Ordering::Relaxed);
    let total_writes = writes_sent.load(Ordering::Relaxed);
    let err_count = write_errors.load(Ordering::Relaxed);
    let events_per_sec = total_events as f64 / elapsed.as_secs_f64();
    let writes_per_sec = total_writes as f64 / elapsed.as_secs_f64();

    let estimate = sampler.finish(events_per_sec);

    eprintln!("\n  SSE RESULTS:");
    eprintln!("  Events received: {total_events}");
    eprintln!("  Writes sent: {total_writes}");
    eprintln!("  Write errors: {err_count}");
    eprintln!("  Duration: {:.2}s", elapsed.as_secs_f64());
    eprintln!("  Events/sec: {:.0}", events_per_sec);
    eprintln!("  Writes/sec: {:.0}", writes_per_sec);
    eprintln!("  Harness CPU: {:.1}% of system", estimate.harness_cpu_fraction * 100.0);

    let result = TestResult {
        test: "sse".to_string(),
        throughput: events_per_sec,
        p50: 0.0,
        p95: 0.0,
        p99: 0.0,
        total: total_events,
        errors: err_count,
        duration_secs: elapsed.as_secs_f64(),
        harness_overhead: estimate.harness_cpu_fraction,
        extrapolated_throughput: format!("{:.0}", estimate.extrapolated_throughput),
        summary: format!("{:.0} events/sec ({} SSE clients, {:.0} writes/sec)", events_per_sec, SSE_CLIENTS, writes_per_sec),
    };

    result.emit();
    result.post_to_test_run().await;

    // No cleanup needed — runner truncates ThroughputTest before each test

    Ok(())
}
