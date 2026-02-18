//! REST Update Load Test
//!
//! Measures PUT update throughput against pre-seeded records.
//!
//! Usage:
//!   cargo run --release --bin load-rest-crud
//!
//! Output: Results written to load/RESULTS.md

use goose::prelude::*;
use load_common::{
    build_seed_record, build_update_record, get_percentiles,
    put_json_record, record_latency, reset_counter, reset_latency_tracker,
    seed_records_parallel, update_section_in_results,
    HarnessSampler, RecordTracker, SeedConfig, TestResult,
    OPTIMAL_VUS, SEED_COMPLETE, SEED_RECORD_COUNT,
    env_duration, env_vus, signal_ready,
};
use std::env;
use std::sync::atomic::Ordering;
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

/// Global record tracker for sharing IDs between transactions
static RECORD_TRACKER: LazyLock<Arc<RecordTracker>> = LazyLock::new(|| {
    Arc::new(RecordTracker::with_capacity(SEED_RECORD_COUNT))
});

#[tokio::main]
async fn main() -> Result<(), GooseError> {
    let args: Vec<String> = env::args().collect();
    let duration = env_duration(60);
    let vus = env_vus(OPTIMAL_VUS);

    // Check if running as Goose subprocess
    if env::var("GOOSE_WORKER").is_ok() || args.iter().any(|a| a.starts_with("--users")) {
        SEED_COMPLETE.store(true, Ordering::SeqCst);
        let ids: Vec<String> = (0..SEED_RECORD_COUNT)
            .map(|i| format!("seed_{i}"))
            .collect();
        RECORD_TRACKER.add_batch(ids);
        return run_update_test(duration, vus).await.map(|_| ());
    }

    eprintln!("=== REST Update Load Test ===");

    // Seed data
    eprintln!("\n--- Seeding {} records ---", SEED_RECORD_COUNT);
    let seed_start = Instant::now();

    let seed_config = SeedConfig::for_app("/benchmarks", "TableName")
        .with_count(SEED_RECORD_COUNT);

    match seed_records_parallel(&seed_config, build_seed_record).await {
        Ok(result) => {
            eprintln!(
                "Seeded {} records in {:.2}s ({:.0} records/sec)",
                result.records_seeded,
                result.duration.as_secs_f64(),
                result.records_per_second
            );
            let ids: Vec<String> = (0..result.records_seeded)
                .map(|i| format!("seed_{i}"))
                .collect();
            RECORD_TRACKER.add_batch(ids);
        }
        Err(e) => {
            eprintln!("Seeding failed: {e}");
        }
    }

    eprintln!("Seeding complete in {:.2}s", seed_start.elapsed().as_secs_f64());
    SEED_COMPLETE.store(true, Ordering::SeqCst);

    // Pre-warm server connections
    eprintln!("\n--- Warming up server ---");
    let warmup_client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .timeout(std::time::Duration::from_secs(10))
        .connect_timeout(std::time::Duration::from_secs(5))
        .build()
        .expect("warmup client");
    load_common::warmup_connections(&warmup_client, "https://localhost:9996/benchmarks/TableName?limit=1", 100).await;

    // Run the load test
    eprintln!("\n--- Running Update Load Test ---");
    signal_ready();
    let sampler = HarnessSampler::start();
    let (total_requests, success_rate, duration_secs, p50, p95, p99) = run_update_test(duration, vus).await?;

    let rps = if duration_secs > 0.0 {
        total_requests as f64 / duration_secs
    } else {
        0.0
    };
    let estimate = sampler.finish(rps);
    let test_result = TestResult {
        test: "rest-update".to_string(),
        throughput: rps,
        p50,
        p95,
        p99,
        total: total_requests,
        errors: ((100.0 - success_rate) / 100.0 * total_requests as f64) as usize,
        duration_secs,
        harness_overhead: estimate.harness_cpu_fraction,
        extrapolated_throughput: format!("{:.0}", estimate.extrapolated_throughput),
        summary: format!("{:.0} req/s, p50={:.2}ms p99={:.2}ms", rps, p50, p99),
    };
    test_result.emit();
    test_result.post_to_test_run().await;

    // Write results to RESULTS.md
    let section = format!(
        "\n**Users**: {} | **Total**: {} | **Duration**: {:.1}s\n\n\
         | p50 | p99 | Throughput | Success |\n\
         |-----|-----|-----------|--------|\n\
         | {:.2}ms | {:.2}ms | {:.0} req/s | {:.1}% |\n\n",
        vus, total_requests, duration_secs,
        p50, p99, rps, success_rate
    );
    update_section_in_results("REST Update Performance", &section);

    Ok(())
}

async fn run_update_test(duration: u64, vus: usize) -> Result<(usize, f64, f64, f64, f64, f64), GooseError> {
    reset_counter();
    reset_latency_tracker();

    let attack = GooseAttack::initialize()?;

    let scenario = scenario!("REST Update")
        .register_transaction(
            transaction!(update_record)
                .set_name("PUT update")
                .set_weight(1)?
        );

    let metrics = attack
        .register_scenario(scenario)
        .set_default(GooseDefault::Host, "https://localhost:9996")?
        .set_default(GooseDefault::AcceptInvalidCerts, true)?
        .set_default(GooseDefault::Users, vus)?
        .set_default(GooseDefault::RunTime, duration as usize)?
        .set_default(GooseDefault::HatchRate, "100")?
        .execute()
        .await?;

    let (p50, p95, p99) = get_percentiles("PUT update");

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

/// PUT update an existing record
async fn update_record(user: &mut GooseUser) -> TransactionResult {
    if !SEED_COMPLETE.load(Ordering::SeqCst) {
        tokio::time::sleep(Duration::from_millis(100)).await;
        return Ok(());
    }

    if let Some(id) = RECORD_TRACKER.random_id() {
        let url = format!("/benchmarks/TableName/{}", id);
        let record = build_update_record(&id, user);
        let start = Instant::now();
        let result = put_json_record(user, &url, record).await;
        record_latency("PUT update", start.elapsed().as_secs_f64() * 1000.0);
        result
    } else {
        Ok(())
    }
}
