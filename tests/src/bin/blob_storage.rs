//! Blob Retrieval Load Test (150KB HTML payloads)
//!
//! Tests large payload (150KB HTML) read throughput.
//! Simulates content delivery use cases.
//!
//! Usage:
//!   cargo run --release --bin load-blob-storage
//!
//! Output: Results written to load/RESULTS.md

use goose::prelude::*;
use load_common::{
    build_seed_blob_record, get_percentiles, get_record,
    record_latency, reset_latency_tracker, update_section_in_results,
    HarnessSampler, RecordTracker, TestResult,
    BLOB_PAYLOAD_SIZE, OPTIMAL_VUS, SEED_COMPLETE,
    env_duration, env_vus, signal_ready,
};
use std::env;
use std::sync::atomic::Ordering;
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

const SEED_COUNT: usize = 50;
const SEED_CONCURRENCY: usize = 10;

/// Blob IDs for read operations
static BLOB_TRACKER: LazyLock<Arc<RecordTracker>> = LazyLock::new(|| {
    Arc::new(RecordTracker::with_capacity(SEED_COUNT))
});

#[tokio::main]
async fn main() -> Result<(), GooseError> {
    let args: Vec<String> = env::args().collect();
    let duration = env_duration(60);
    let vus = env_vus(OPTIMAL_VUS);

    // Check if running as Goose subprocess
    if env::var("GOOSE_WORKER").is_ok() || args.iter().any(|a| a.starts_with("--users")) {
        SEED_COMPLETE.store(true, Ordering::SeqCst);
        let ids: Vec<String> = (0..SEED_COUNT).map(|i| format!("blob_{i}")).collect();
        BLOB_TRACKER.add_batch(ids);
        return run_blob_test(duration, vus).await.map(|_| ());
    }

    let payload_kb = BLOB_PAYLOAD_SIZE / 1024;
    eprintln!("=== Blob Retrieval Load Test ===");
    eprintln!("Payload: {}KB HTML, 100% reads", payload_kb);

    // Seed blob data (sequential — 150KB payloads are too heavy for parallel writes)
    eprintln!("\n--- Seeding {} blob records ({}KB each) ---", SEED_COUNT, payload_kb);
    let seed_start = Instant::now();

    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .timeout(Duration::from_secs(30))
        .connect_timeout(Duration::from_secs(5))
        .build()
        .expect("Failed to build HTTP client");

    let seeded = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut tasks = tokio::task::JoinSet::new();

    for i in 0..SEED_COUNT {
        let client = client.clone();
        let seeded = seeded.clone();
        tasks.spawn(async move {
            let record = build_seed_blob_record(i);
            let url = "https://localhost:9996/benchmarks/HtmlPage";
            match client
                .post(url)
                .header("Content-Type", "application/json")
                .body(record.to_string())
                .send()
                .await
            {
                Ok(resp) if resp.status().is_success() => {
                    BLOB_TRACKER.add(format!("blob_{i}"));
                    seeded.fetch_add(1, Ordering::Relaxed);
                }
                Ok(resp) => {
                    eprintln!("  Seed failed: {} (HTTP {})", i, resp.status());
                }
                Err(e) => {
                    eprintln!("  Seed error: {} - {}", i, e);
                }
            }
        });

        // Limit concurrency
        if tasks.len() >= SEED_CONCURRENCY {
            tasks.join_next().await;
        }
    }
    while tasks.join_next().await.is_some() {}
    let seeded = seeded.load(Ordering::Relaxed);

    eprintln!("Seeded {} blob records in {:.2}s", seeded, seed_start.elapsed().as_secs_f64());
    SEED_COMPLETE.store(true, Ordering::SeqCst);

    // Pre-warm server connections
    eprintln!("\n--- Warming up server ---");
    let warmup_client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .timeout(Duration::from_secs(10))
        .connect_timeout(Duration::from_secs(5))
        .build()
        .expect("warmup client");
    load_common::warmup_connections(&warmup_client, "https://localhost:9996/benchmarks/HtmlPage?limit=1", 100).await;

    // Run the load test
    eprintln!("\n--- Running Blob Retrieval Test ---");
    signal_ready();
    let sampler = HarnessSampler::start();
    let (total_requests, success_rate, duration_secs, p50, p95, p99) = run_blob_test(duration, vus).await?;

    let rps = if duration_secs > 0.0 {
        total_requests as f64 / duration_secs
    } else {
        0.0
    };
    let estimate = sampler.finish(rps);
    let read_mb_s = (total_requests as f64 * payload_kb as f64 / 1024.0) / duration_secs;

    let test_result = TestResult {
        test: "blob-retrieval".to_string(),
        throughput: rps,
        p50,
        p95,
        p99,
        total: total_requests,
        errors: ((100.0 - success_rate) / 100.0 * total_requests as f64) as usize,
        duration_secs,
        harness_overhead: estimate.harness_cpu_fraction,
        extrapolated_throughput: format!("{:.0}", estimate.extrapolated_throughput),
        summary: format!("{:.0} req/s, {:.1}MB/s ({}KB payloads)", rps, read_mb_s, payload_kb),
    };
    test_result.emit();
    test_result.post_to_test_run().await;

    let section = format!(
        "\n**Payload**: {}KB HTML | **Users**: {} | **Total**: {} | **Duration**: {:.1}s\n\n\
         | p50 | p99 | Throughput | MB/s | Success |\n\
         |-----|-----|-----------|------|--------|\n\
         | {:.2}ms | {:.2}ms | {:.0} req/s | {:.1} | {:.1}% |\n\n",
        payload_kb, vus, total_requests, duration_secs,
        p50, p99, rps, read_mb_s, success_rate
    );
    update_section_in_results("Blob Retrieval Performance", &section);

    Ok(())
}

async fn run_blob_test(duration: u64, vus: usize) -> Result<(usize, f64, f64, f64, f64, f64), GooseError> {
    reset_latency_tracker();

    let attack = GooseAttack::initialize()?;

    let scenario = scenario!("Blob Retrieval")
        .register_transaction(
            transaction!(read_blob)
                .set_name("GET blob (150KB)")
                .set_weight(1)?
        );

    let metrics = attack
        .register_scenario(scenario)
        .set_default(GooseDefault::Host, "https://localhost:9996")?
        .set_default(GooseDefault::AcceptInvalidCerts, true)?
        .set_default(GooseDefault::Users, vus)?
        .set_default(GooseDefault::RunTime, duration as usize)?
        .set_default(GooseDefault::HatchRate, "50")?
        .execute()
        .await?;

    let (p50, p95, p99) = get_percentiles("GET blob (150KB)");

    let mut total_success = 0usize;
    let mut total_fail = 0usize;
    for (_, agg) in metrics.requests.iter() {
        total_success += agg.success_count;
        total_fail += agg.fail_count;
    }
    let total = total_success + total_fail;
    let success_rate = if total > 0 {
        (total_success as f64 / total as f64) * 100.0
    } else {
        100.0
    };

    Ok((total, success_rate, metrics.duration as f64, p50, p95, p99))
}

/// Read a 150KB blob
async fn read_blob(user: &mut GooseUser) -> TransactionResult {
    if !SEED_COMPLETE.load(Ordering::SeqCst) {
        tokio::time::sleep(Duration::from_millis(100)).await;
        return Ok(());
    }

    if let Some(id) = BLOB_TRACKER.random_id() {
        let url = format!("/benchmarks/HtmlPage/{}", id);
        let start = Instant::now();
        let result = get_record(user, &url).await;
        record_latency("GET blob (150KB)", start.elapsed().as_secs_f64() * 1000.0);
        result
    } else {
        Ok(())
    }
}
