//! GraphQL Load Test
//!
//! Tests GraphQL read queries or mutations separately.
//!
//! Usage:
//!   cargo run --release --bin load-graphql -- read       # Query with 1 join
//!   cargo run --release --bin load-graphql -- mutation    # Create mutations
//!
//! Output: Results written to load/RESULTS.md

use goose::prelude::*;
use load_common::{
    build_seed_record, get_percentiles, graphql_request,
    record_latency, reset_latency_tracker, seed_records_parallel,
    seed_relationship_data, update_section_in_results,
    HarnessSampler, RecordTracker, SeedConfig, TestResult,
    OPTIMAL_VUS, SEED_COMPLETE, SEED_RECORD_COUNT,
    env_duration, env_vus, signal_ready,
};
use std::env;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

/// Global record tracker for GraphQL queries
static RECORD_TRACKER: LazyLock<Arc<RecordTracker>> = LazyLock::new(|| {
    Arc::new(RecordTracker::with_capacity(SEED_RECORD_COUNT))
});

/// Counter for unique IDs in mutations
static MUTATION_COUNTER: AtomicU64 = AtomicU64::new(0);

#[tokio::main]
async fn main() -> Result<(), GooseError> {
    // Mode passed via GRAPHQL_MODE env var (runner sets this)
    let mode = env::var("GRAPHQL_MODE").unwrap_or_else(|_| "read".to_string());
    let mode = mode.as_str();
    let duration = env_duration(60);
    let vus = env_vus(OPTIMAL_VUS);

    let args: Vec<String> = env::args().collect();

    // Check if running as Goose subprocess
    if env::var("GOOSE_WORKER").is_ok() || args.iter().any(|a| a.starts_with("--users")) {
        SEED_COMPLETE.store(true, Ordering::SeqCst);
        let ids: Vec<String> = if mode == "mutation" {
            (0..SEED_RECORD_COUNT).map(|i| format!("seed_{i}")).collect()
        } else {
            (0..1000).map(|i| format!("prod_{i}")).collect()
        };
        RECORD_TRACKER.add_batch(ids);
        return run_graphql_test(mode, duration, vus).await.map(|_| ());
    }

    let test_name = if mode == "mutation" { "graphql-mutation" } else { "graphql-read" };
    eprintln!("=== GraphQL Load Test ({}) ===", mode);

    // Seed data — reads need Product+Brand relationships, mutations use their own table
    if mode == "mutation" {
        eprintln!("\n--- Seeding {} records ---", SEED_RECORD_COUNT);
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
    } else {
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
            }
        }
        let ids: Vec<String> = (0..1000).map(|i| format!("prod_{i}")).collect();
        RECORD_TRACKER.add_batch(ids);
    }

    SEED_COMPLETE.store(true, Ordering::SeqCst);

    // Pre-warm server connections
    eprintln!("\n--- Warming up server ---");
    let warmup_client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .timeout(std::time::Duration::from_secs(10))
        .connect_timeout(std::time::Duration::from_secs(5))
        .build()
        .expect("warmup client");
    let warmup_table = if mode == "mutation" { "TableName" } else { "Product" };
    let warmup_url = format!("https://localhost:9996/benchmarks/{}?limit=1", warmup_table);
    load_common::warmup_connections(&warmup_client, &warmup_url, 100).await;

    // Run the load test
    eprintln!("\n--- Running GraphQL {} Test ---", mode);
    signal_ready();
    let sampler = HarnessSampler::start();
    let (total_requests, success_rate, duration_secs, p50, p95, p99) = run_graphql_test(mode, duration, vus).await?;

    let rps = if duration_secs > 0.0 {
        total_requests as f64 / duration_secs
    } else {
        0.0
    };
    let estimate = sampler.finish(rps);
    let test_result = TestResult {
        test: test_name.to_string(),
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

    let section_title = if mode == "mutation" {
        "GraphQL Mutation Performance"
    } else {
        "GraphQL Read Performance"
    };
    let section = format!(
        "\n**Users**: {} | **Total**: {} | **Duration**: {:.1}s\n\n\
         | p50 | p99 | Throughput | Success |\n\
         |-----|-----|-----------|--------|\n\
         | {:.2}ms | {:.2}ms | {:.0} req/s | {:.1}% |\n\n",
        vus, total_requests, duration_secs,
        p50, p99, rps, success_rate
    );
    update_section_in_results(section_title, &section);

    Ok(())
}

async fn run_graphql_test(mode: &str, duration: u64, vus: usize) -> Result<(usize, f64, f64, f64, f64, f64), GooseError> {
    reset_latency_tracker();

    let attack = GooseAttack::initialize()?;
    let op_name = if mode == "mutation" { "GraphQL mutation" } else { "GraphQL read" };

    let scenario = if mode == "mutation" {
        scenario!("GraphQL Mutations")
            .register_transaction(
                transaction!(graphql_mutation_create)
                    .set_name("GraphQL mutation")
                    .set_weight(1)?
            )
    } else {
        scenario!("GraphQL Reads")
            .register_transaction(
                transaction!(graphql_query_with_join)
                    .set_name("GraphQL read")
                    .set_weight(1)?
            )
    };

    let metrics = attack
        .register_scenario(scenario)
        .set_default(GooseDefault::Host, "https://localhost:9996")?
        .set_default(GooseDefault::AcceptInvalidCerts, true)?
        .set_default(GooseDefault::Users, vus)?
        .set_default(GooseDefault::RunTime, duration as usize)?
        .set_default(GooseDefault::HatchRate, "100")?
        .execute()
        .await?;

    let (p50, p95, p99) = get_percentiles(op_name);

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

/// GraphQL query: Product with 1 join to Brand, 1 field each
async fn graphql_query_with_join(user: &mut GooseUser) -> TransactionResult {
    if !SEED_COMPLETE.load(Ordering::SeqCst) {
        tokio::time::sleep(Duration::from_millis(100)).await;
        return Ok(());
    }

    let Some(id) = RECORD_TRACKER.random_id() else {
        return Ok(());
    };

    let query = r#"
        query GetProduct($id: ID!) {
            Product(id: $id) {
                name
                brand {
                    name
                }
            }
        }
    "#;

    let variables = serde_json::json!({ "id": id });

    let start = Instant::now();
    let result = graphql_request(user, "/benchmarks/graphql", query, Some(variables)).await;
    record_latency("GraphQL read", start.elapsed().as_secs_f64() * 1000.0);
    result
}

/// GraphQL mutation: create a Product record
async fn graphql_mutation_create(user: &mut GooseUser) -> TransactionResult {
    let counter = MUTATION_COUNTER.fetch_add(1, Ordering::SeqCst);
    let user_id = user.weighted_users_index;
    let id = format!("gql_{user_id}_{counter}");

    let mutation = r#"
        mutation CreateProduct($data: String) {
            createProduct(data: $data) {
                name
            }
        }
    "#;

    let variables = serde_json::json!({
        "data": {
            "id": id,
            "name": format!("Product {}", counter),
            "brandId": format!("brand_{}", counter % 100),
            "price": 9.99,
            "inStock": true
        }
    });

    let start = Instant::now();
    let result = graphql_request(user, "/benchmarks/graphql", mutation, Some(variables)).await;
    record_latency("GraphQL mutation", start.elapsed().as_secs_f64() * 1000.0);
    result
}
