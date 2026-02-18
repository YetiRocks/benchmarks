//! REST Join Load Test
//!
//! Tests single-join performance: Product -> Brand, 1 field each.
//! Uses the `select` parameter for nested field traversal.
//!
//! Usage:
//!   cargo run --release --bin load-rest-relationships
//!
//! Output: Results written to load/RESULTS.md

use goose::prelude::*;
use load_common::{
    get_percentiles, get_record, record_latency, reset_latency_tracker,
    seed_relationship_data, update_section_in_results,
    HarnessSampler, RecordTracker, TestResult,
    OPTIMAL_VUS, SEED_COMPLETE,
    env_duration, env_vus, signal_ready,
};
use std::env;
use std::sync::atomic::Ordering;
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

/// Product IDs for join queries
static PRODUCT_TRACKER: LazyLock<Arc<RecordTracker>> = LazyLock::new(|| {
    Arc::new(RecordTracker::with_capacity(1000))
});

#[tokio::main]
async fn main() -> Result<(), GooseError> {
    let args: Vec<String> = env::args().collect();
    let duration = env_duration(60);
    let vus = env_vus(OPTIMAL_VUS);

    // Check if running as Goose subprocess
    if env::var("GOOSE_WORKER").is_ok() || args.iter().any(|a| a.starts_with("--users")) {
        SEED_COMPLETE.store(true, Ordering::SeqCst);
        let ids: Vec<String> = (0..1000).map(|i| format!("prod_{i}")).collect();
        PRODUCT_TRACKER.add_batch(ids);
        return run_join_test(duration, vus).await.map(|_| ());
    }

    eprintln!("=== REST Join Load Test ===");
    eprintln!("Query: Product.name + Brand.name (1 join, 1 field each)");

    // Seed relationship data
    eprintln!("\n--- Seeding relationship data ---");
    eprintln!("  10 Manufacturers -> 100 Brands -> 1000 Products");

    match seed_relationship_data("https://localhost:9996", "/benchmarks").await {
        Ok(result) => {
            eprintln!(
                "Seeded {} records in {:.2}s ({:.0} records/sec)",
                result.records_seeded,
                result.duration.as_secs_f64(),
                result.records_per_second
            );
        }
        Err(e) => {
            eprintln!("Seeding failed: {e}");
            return Err(GooseError::InvalidOption {
                option: "seed".to_string(),
                value: e.to_string(),
                detail: "Failed to seed relationship data".to_string(),
            });
        }
    }

    let ids: Vec<String> = (0..1000).map(|i| format!("prod_{i}")).collect();
    PRODUCT_TRACKER.add_batch(ids);
    SEED_COMPLETE.store(true, Ordering::SeqCst);

    // Pre-warm server connections
    eprintln!("\n--- Warming up server ---");
    let warmup_client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .timeout(std::time::Duration::from_secs(10))
        .connect_timeout(std::time::Duration::from_secs(5))
        .build()
        .expect("warmup client");
    load_common::warmup_connections(&warmup_client, "https://localhost:9996/benchmarks/Product?limit=1", 100).await;

    // Run the load test
    eprintln!("\n--- Running REST Join Load Test ---");
    signal_ready();
    let sampler = HarnessSampler::start();
    let (total_requests, success_rate, duration_secs, p50, p95, p99) = run_join_test(duration, vus).await?;

    let rps = if duration_secs > 0.0 {
        total_requests as f64 / duration_secs
    } else {
        0.0
    };
    let estimate = sampler.finish(rps);
    let test_result = TestResult {
        test: "rest-join".to_string(),
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

    let section = format!(
        "\n**Query**: `Product.name + Brand.name` (1 join) | **Users**: {} | **Total**: {} | **Duration**: {:.1}s\n\n\
         | p50 | p99 | Throughput | Success |\n\
         |-----|-----|-----------|--------|\n\
         | {:.2}ms | {:.2}ms | {:.0} req/s | {:.1}% |\n\n",
        vus, total_requests, duration_secs,
        p50, p99, rps, success_rate
    );
    update_section_in_results("REST Join Performance", &section);

    Ok(())
}

async fn run_join_test(duration: u64, vus: usize) -> Result<(usize, f64, f64, f64, f64, f64), GooseError> {
    reset_latency_tracker();

    let attack = GooseAttack::initialize()?;

    let scenario = scenario!("REST Join")
        .register_transaction(
            transaction!(get_product_with_brand)
                .set_name("GET Product+Brand")
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

    let (p50, p95, p99) = get_percentiles("GET Product+Brand");

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

/// GET Product with Brand join: select=name,brand{name}
async fn get_product_with_brand(user: &mut GooseUser) -> TransactionResult {
    if !SEED_COMPLETE.load(Ordering::SeqCst) {
        tokio::time::sleep(Duration::from_millis(100)).await;
        return Ok(());
    }

    if let Some(id) = PRODUCT_TRACKER.random_id() {
        // select=name,brand{name} — 1 field from Product, 1 field from Brand
        // URL-encode braces: { = %7B, } = %7D
        let url = format!(
            "/benchmarks/Product/{}?select=name,brand%7Bname%7D",
            id
        );
        let start = Instant::now();
        let result = get_record(user, &url).await;
        record_latency("GET Product+Brand", start.elapsed().as_secs_f64() * 1000.0);
        result
    } else {
        Ok(())
    }
}
