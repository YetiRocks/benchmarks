//! Focused Throughput Test
//!
//! Measures maximum achievable read/write throughput without Goose overhead.
//! Uses reqwest with connection pooling and harness overhead estimation.
//!
//! Usage:
//!   cargo run --release --bin throughput          # Run all tests
//!   cargo run --release --bin throughput -- read  # Read-only test
//!   cargo run --release --bin throughput -- write # Write-only test

use load_common::{
    APP_PATH, BASE_URL, CONCURRENT_TASKS, POOL_IDLE_TIMEOUT_SECS, SEED_RECORD_COUNT,
    TARGET_READ_THROUGHPUT, TARGET_WRITE_THROUGHPUT, TCP_KEEPALIVE_SECS, TEST_DURATION_SECS,
    WARMUP_REQUESTS, HarnessSampler, TestResult, warmup_connections,
    env_duration, env_vus, signal_ready,
};
use reqwest::Client;
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::JoinSet;

const TABLE: &str = "ThroughputTest";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let mode = args.get(1).map(|s| s.as_str()).unwrap_or("all");

    let duration = env_duration(TEST_DURATION_SECS);
    let vus = env_vus(CONCURRENT_TASKS);

    let client = Client::builder()
        .danger_accept_invalid_certs(true)
        .timeout(Duration::from_secs(10))
        .connect_timeout(Duration::from_secs(5))
        .pool_max_idle_per_host(vus)
        .pool_idle_timeout(Duration::from_secs(POOL_IDLE_TIMEOUT_SECS))
        .tcp_keepalive(Duration::from_secs(TCP_KEEPALIVE_SECS))
        .build()?;

    eprintln!("=== Yeti Throughput Test ===");
    eprintln!("Target: {BASE_URL}{APP_PATH}/{TABLE}");
    eprintln!("Concurrent tasks: {vus}");
    eprintln!("Test duration: {duration}s per test\n");

    // Pre-warm connections (TLS + HTTP/2 negotiation)
    let warmup_url = format!("{BASE_URL}{APP_PATH}/{TABLE}/rec_0");
    warmup_connections(&client, &warmup_url, 200).await;

    // Seed data for read tests
    eprintln!("--- Seeding {SEED_RECORD_COUNT} records ---");
    seed_data(&client, SEED_RECORD_COUNT).await?;
    eprintln!("Seeding complete\n");

    signal_ready();

    match mode {
        "read" => {
            let result = run_read_test(&client, duration, vus).await?;
            result.emit();
            result.post_to_test_run().await;
        }
        "write" => {
            let result = run_write_test(&client, duration, vus).await?;
            result.emit();
            result.post_to_test_run().await;
        }
        _ => {
            let read_result = run_read_test(&client, duration, vus).await?;
            read_result.emit();
            read_result.post_to_test_run().await;

            eprintln!();

            let write_result = run_write_test(&client, duration, vus).await?;
            write_result.emit();
            write_result.post_to_test_run().await;
        }
    }

    Ok(())
}

async fn seed_data(client: &Client, count: usize) -> Result<(), Box<dyn std::error::Error>> {
    let url = format!("{BASE_URL}{APP_PATH}/{TABLE}");

    let start = Instant::now();
    let mut tasks = JoinSet::new();

    for i in 0..count {
        let client = client.clone();
        let url = url.clone();
        tasks.spawn(async move {
            let record = json!({
                "id": format!("rec_{i}"),
                "name": format!("Record {i}"),
                "value": i
            });
            client.post(&url).json(&record).send().await
        });

        if tasks.len() >= 50 {
            tasks.join_next().await;
        }
    }

    while tasks.join_next().await.is_some() {}

    let elapsed = start.elapsed();
    eprintln!(
        "  Seeded {} records in {:.2}s ({:.0} rec/s)",
        count,
        elapsed.as_secs_f64(),
        count as f64 / elapsed.as_secs_f64()
    );

    Ok(())
}

async fn run_read_test(client: &Client, duration: u64, vus: usize) -> Result<TestResult, Box<dyn std::error::Error>> {
    eprintln!("--- Read Throughput Test ---");

    // Warmup
    eprintln!("  Warming up ({WARMUP_REQUESTS} requests)...");
    let warmup_counter = Arc::new(AtomicUsize::new(0));
    run_concurrent_reads(client, WARMUP_REQUESTS, &warmup_counter, vus).await;

    // Start harness CPU sampling
    let sampler = HarnessSampler::start();

    // Actual test
    eprintln!("  Running for {duration}s...");
    let counter = Arc::new(AtomicUsize::new(0));
    let errors = Arc::new(AtomicUsize::new(0));
    let start = Instant::now();
    let deadline = start + Duration::from_secs(duration);

    // Pre-compute URL pool to avoid per-request format!()
    let urls: Arc<Vec<String>> = Arc::new(
        (0..SEED_RECORD_COUNT)
            .map(|id| format!("{BASE_URL}{APP_PATH}/{TABLE}/rec_{id}"))
            .collect(),
    );

    let mut tasks: JoinSet<Vec<f64>> = JoinSet::new();

    for _ in 0..vus {
        let client = client.clone();
        let counter = counter.clone();
        let errors = errors.clone();
        let urls = urls.clone();

        tasks.spawn(async move {
            let mut latencies = Vec::with_capacity(64_000);
            let mut i = 0u64;
            while Instant::now() < deadline {
                let url = &urls[(i % urls.len() as u64) as usize];
                let t0 = Instant::now();
                match client.get(url).send().await {
                    Ok(resp) if resp.status().is_success() => {
                        latencies.push(t0.elapsed().as_secs_f64() * 1000.0);
                        counter.fetch_add(1, Ordering::Relaxed);
                    }
                    _ => {
                        errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
                i += 1;
            }
            latencies
        });
    }

    // Collect all per-VU latencies
    let mut all_latencies: Vec<f64> = Vec::new();
    while let Some(Ok(lats)) = tasks.join_next().await {
        all_latencies.extend(lats);
    }

    let elapsed = start.elapsed();
    let total = counter.load(Ordering::Relaxed);
    let err_count = errors.load(Ordering::Relaxed);
    let rps = total as f64 / elapsed.as_secs_f64();
    let target_pct = (rps / TARGET_READ_THROUGHPUT as f64) * 100.0;

    // Compute percentiles
    let (p50, p95, p99) = compute_percentiles(&mut all_latencies);

    // Compute harness overhead
    let estimate = sampler.finish(rps);

    eprintln!("\n  READ RESULTS:");
    eprintln!("  Total requests: {total}");
    eprintln!("  Errors: {err_count}");
    eprintln!("  Duration: {:.2}s", elapsed.as_secs_f64());
    eprintln!("  Throughput: {:.0} req/s", rps);
    eprintln!("  Latency: p50={:.2}ms p95={:.2}ms p99={:.2}ms", p50, p95, p99);
    eprintln!("  Target ({TARGET_READ_THROUGHPUT}): {target_pct:.1}%");
    eprintln!("  Harness CPU: {:.1}% of system", estimate.harness_cpu_fraction * 100.0);
    eprintln!("  Extrapolated (separate machines): {:.0} req/s", estimate.extrapolated_throughput);

    Ok(TestResult {
        test: "rest-read".to_string(),
        throughput: rps,
        p50,
        p95,
        p99,
        total,
        errors: err_count,
        duration_secs: elapsed.as_secs_f64(),
        harness_overhead: estimate.harness_cpu_fraction,
        extrapolated_throughput: format!("{:.0}", estimate.extrapolated_throughput),
        summary: format!("{:.0} req/s, p50={:.2}ms p95={:.2}ms p99={:.2}ms", rps, p50, p95, p99),
    })
}

async fn run_write_test(client: &Client, duration: u64, vus: usize) -> Result<TestResult, Box<dyn std::error::Error>> {
    eprintln!("--- Write Throughput Test ---");

    // Warmup
    eprintln!("  Warming up ({WARMUP_REQUESTS} requests)...");
    let warmup_counter = Arc::new(AtomicUsize::new(0));
    run_concurrent_writes(client, WARMUP_REQUESTS, &warmup_counter, vus).await;

    // Start harness CPU sampling
    let sampler = HarnessSampler::start();

    // Actual test
    eprintln!("  Running for {duration}s...");
    let counter = Arc::new(AtomicUsize::new(0));
    let errors = Arc::new(AtomicUsize::new(0));
    let start = Instant::now();
    let deadline = start + Duration::from_secs(duration);

    let mut tasks: JoinSet<Vec<f64>> = JoinSet::new();

    let write_url = format!("{BASE_URL}{APP_PATH}/{TABLE}");

    for task_id in 0..vus {
        let client = client.clone();
        let counter = counter.clone();
        let errors = errors.clone();
        let url = write_url.clone();

        tasks.spawn(async move {
            let mut latencies = Vec::with_capacity(64_000);
            let mut i = 0u64;
            while Instant::now() < deadline {
                // Pre-serialized JSON via format!() — avoids serde overhead
                let body = format!(
                    r#"{{"id":"w{task_id}_{i}","name":"t{i}","value":{i}}}"#,
                    task_id = task_id,
                    i = i
                );

                let t0 = Instant::now();
                match client
                    .post(&url)
                    .header("Content-Type", "application/json")
                    .body(body)
                    .send()
                    .await
                {
                    Ok(resp) if resp.status().is_success() => {
                        latencies.push(t0.elapsed().as_secs_f64() * 1000.0);
                        counter.fetch_add(1, Ordering::Relaxed);
                    }
                    _ => {
                        errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
                i += 1;
            }
            latencies
        });
    }

    // Collect all per-VU latencies
    let mut all_latencies: Vec<f64> = Vec::new();
    while let Some(Ok(lats)) = tasks.join_next().await {
        all_latencies.extend(lats);
    }

    let elapsed = start.elapsed();
    let total = counter.load(Ordering::Relaxed);
    let err_count = errors.load(Ordering::Relaxed);
    let wps = total as f64 / elapsed.as_secs_f64();
    let target_pct = (wps / TARGET_WRITE_THROUGHPUT as f64) * 100.0;

    // Compute percentiles
    let (p50, p95, p99) = compute_percentiles(&mut all_latencies);

    // Compute harness overhead
    let estimate = sampler.finish(wps);

    eprintln!("\n  WRITE RESULTS:");
    eprintln!("  Total requests: {total}");
    eprintln!("  Errors: {err_count}");
    eprintln!("  Duration: {:.2}s", elapsed.as_secs_f64());
    eprintln!("  Throughput: {:.0} req/s", wps);
    eprintln!("  Latency: p50={:.2}ms p95={:.2}ms p99={:.2}ms", p50, p95, p99);
    eprintln!("  Target ({TARGET_WRITE_THROUGHPUT}): {target_pct:.1}%");
    eprintln!("  Harness CPU: {:.1}% of system", estimate.harness_cpu_fraction * 100.0);
    eprintln!("  Extrapolated (separate machines): {:.0} req/s", estimate.extrapolated_throughput);

    Ok(TestResult {
        test: "rest-write".to_string(),
        throughput: wps,
        p50,
        p95,
        p99,
        total,
        errors: err_count,
        duration_secs: elapsed.as_secs_f64(),
        harness_overhead: estimate.harness_cpu_fraction,
        extrapolated_throughput: format!("{:.0}", estimate.extrapolated_throughput),
        summary: format!("{:.0} req/s, p50={:.2}ms p95={:.2}ms p99={:.2}ms", wps, p50, p95, p99),
    })
}

async fn run_concurrent_reads(client: &Client, count: usize, counter: &Arc<AtomicUsize>, vus: usize) {
    let mut tasks = JoinSet::new();

    for i in 0..count {
        let client = client.clone();
        let counter = counter.clone();

        tasks.spawn(async move {
            let id = i % SEED_RECORD_COUNT;
            let url = format!("{BASE_URL}{APP_PATH}/{TABLE}/rec_{id}");
            if client.get(&url).send().await.is_ok() {
                counter.fetch_add(1, Ordering::Relaxed);
            }
        });

        if tasks.len() >= vus {
            tasks.join_next().await;
        }
    }

    while tasks.join_next().await.is_some() {}
}

async fn run_concurrent_writes(client: &Client, count: usize, counter: &Arc<AtomicUsize>, vus: usize) {
    let url = format!("{BASE_URL}{APP_PATH}/{TABLE}");
    let mut tasks = JoinSet::new();

    for i in 0..count {
        let client = client.clone();
        let counter = counter.clone();
        let url = url.clone();

        tasks.spawn(async move {
            let body = format!(
                r#"{{"id":"warmup_{i}","name":"w{i}","value":{i}}}"#,
                i = i
            );
            if client
                .post(&url)
                .header("Content-Type", "application/json")
                .body(body)
                .send()
                .await
                .is_ok()
            {
                counter.fetch_add(1, Ordering::Relaxed);
            }
        });

        if tasks.len() >= vus {
            tasks.join_next().await;
        }
    }

    while tasks.join_next().await.is_some() {}
}

/// Sort latencies and compute p50, p95, p99
fn compute_percentiles(latencies: &mut [f64]) -> (f64, f64, f64) {
    if latencies.is_empty() {
        return (0.0, 0.0, 0.0);
    }
    latencies.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let len = latencies.len();
    let p50 = latencies[(len as f64 * 0.50) as usize];
    let p95 = latencies[((len as f64 * 0.95) as usize).min(len - 1)];
    let p99 = latencies[((len as f64 * 0.99) as usize).min(len - 1)];
    (p50, p95, p99)
}
