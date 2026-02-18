//! Vector Search Load Test
//!
//! Seeds documents with embeddings, then measures vector search throughput
//! using random word queries against the HNSW index. Each search query
//! sends a text string that gets embedded and matched against stored vectors.
//!
//! Usage:
//!   cargo run --release --bin load-vector-search

use load_common::{
    APP_PATH, BASE_URL,
    POOL_IDLE_TIMEOUT_SECS, TCP_KEEPALIVE_SECS,
    HarnessSampler, TestResult, warmup_connections,
    env_duration, env_vus, signal_ready,
};
use rand::Rng;
use reqwest::Client;
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::JoinSet;

const TABLE: &str = "VectorDoc";
const SEED_COUNT: usize = 200;
const SEARCH_VUS: usize = 10;

/// Word bank for generating seed documents and search queries
const WORDS: &[&str] = &[
    "algorithm", "network", "database", "machine", "learning", "vector",
    "search", "index", "query", "embedding", "neural", "transformer",
    "attention", "gradient", "optimization", "inference", "training",
    "classification", "regression", "clustering", "dimension", "feature",
    "model", "prediction", "accuracy", "latency", "throughput", "benchmark",
    "parallel", "distributed", "scalable", "efficient", "robust", "semantic",
    "similarity", "distance", "cosine", "euclidean", "approximate", "nearest",
    "neighbor", "graph", "layer", "tensor", "matrix", "compute", "memory",
    "storage", "retrieval", "ranking", "relevance", "precision", "recall",
    "performance", "architecture", "pipeline", "deployment", "production",
    "container", "orchestration", "monitoring", "telemetry", "observability",
];

/// Fixed query pool — small enough for embedding cache to stay warm,
/// large enough for realistic variety (50 distinct queries)
const QUERY_POOL_SIZE: usize = 50;

/// Build a fixed pool of search queries at startup
fn build_query_pool() -> Vec<String> {
    let mut rng = rand::thread_rng();
    (0..QUERY_POOL_SIZE)
        .map(|_| {
            let count = 2 + rng.gen_range(0..4);
            (0..count)
                .map(|_| WORDS[rng.gen_range(0..WORDS.len())])
                .collect::<Vec<_>>()
                .join(" ")
        })
        .collect()
}

/// Generate a seed document with ~50 words
fn seed_document(i: usize) -> serde_json::Value {
    let mut rng = rand::thread_rng();
    let word_count = 40 + rng.gen_range(0..20);
    let content: String = (0..word_count)
        .map(|_| WORDS[rng.gen_range(0..WORDS.len())])
        .collect::<Vec<_>>()
        .join(" ");

    let title: String = (0..5)
        .map(|_| WORDS[rng.gen_range(0..WORDS.len())])
        .collect::<Vec<_>>()
        .join(" ");

    json!({
        "id": format!("seed_vec_{i}"),
        "title": title,
        "content": content,
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let duration = env_duration(10);
    let vus = env_vus(SEARCH_VUS);

    let client = Client::builder()
        .danger_accept_invalid_certs(true)
        .pool_max_idle_per_host(vus.max(20))
        .pool_idle_timeout(Duration::from_secs(POOL_IDLE_TIMEOUT_SECS))
        .tcp_keepalive(Duration::from_secs(TCP_KEEPALIVE_SECS))
        .build()?;

    eprintln!("=== Vector Search Load Test ===");
    eprintln!("Target: {BASE_URL}{APP_PATH}/{TABLE}");
    eprintln!("Concurrent tasks: {vus}");
    eprintln!("Test duration: {duration}s\n");

    // Seed documents (each POST triggers embedding generation)
    eprintln!("--- Seeding {SEED_COUNT} documents with embeddings ---");
    let seed_start = Instant::now();
    seed_data(&client, SEED_COUNT).await?;
    eprintln!(
        "  Seeded {} docs in {:.1}s ({:.1} docs/s)\n",
        SEED_COUNT,
        seed_start.elapsed().as_secs_f64(),
        SEED_COUNT as f64 / seed_start.elapsed().as_secs_f64()
    );

    // Build fixed query pool and warm the embedding cache
    let query_pool = Arc::new(build_query_pool());
    eprintln!("--- Warming up search path ({} queries) ---", query_pool.len());
    warmup_connections(&client, &format!("{BASE_URL}{APP_PATH}/{TABLE}?limit=1"), 50).await;
    for q in query_pool.iter() {
        let url = format!(
            "{BASE_URL}{APP_PATH}/{TABLE}?vector_attr=embedding&vector_text={}&vector_model=all-MiniLM-L6-v2&limit=5",
            urlencoded(q)
        );
        let _ = client.get(&url).send().await;
    }
    eprintln!("  Warmup complete — embedding cache primed\n");

    signal_ready();

    let result = run_search_test(&client, duration, vus, &query_pool).await?;
    result.emit();
    result.post_to_test_run().await;

    Ok(())
}

async fn seed_data(client: &Client, count: usize) -> Result<(), Box<dyn std::error::Error>> {
    let url = format!("{BASE_URL}{APP_PATH}/{TABLE}");
    let mut tasks = JoinSet::new();

    for i in 0..count {
        let client = client.clone();
        let url = url.clone();
        let doc = seed_document(i);

        tasks.spawn(async move {
            client.post(&url)
                .header("Content-Type", "application/json")
                .json(&doc)
                .send()
                .await
        });

        // Limit concurrency — each POST triggers embedding, so don't overwhelm
        if tasks.len() >= 10 {
            tasks.join_next().await;
        }
    }

    while tasks.join_next().await.is_some() {}
    Ok(())
}

async fn run_search_test(client: &Client, duration: u64, vus: usize, query_pool: &Arc<Vec<String>>) -> Result<TestResult, Box<dyn std::error::Error>> {
    eprintln!("--- Vector Search Throughput Test ---");

    let sampler = HarnessSampler::start();

    eprintln!("  Running for {duration}s...");
    let counter = Arc::new(AtomicUsize::new(0));
    let errors = Arc::new(AtomicUsize::new(0));
    let start = Instant::now();
    let deadline = start + Duration::from_secs(duration);

    let mut tasks: JoinSet<Vec<f64>> = JoinSet::new();

    for _task_id in 0..vus {
        let client = client.clone();
        let counter = counter.clone();
        let errors = errors.clone();
        let queries = query_pool.clone();

        tasks.spawn(async move {
            let mut latencies = Vec::with_capacity(10_000);
            let mut i = 0usize;
            while Instant::now() < deadline {
                let query = &queries[i % queries.len()];
                i += 1;
                let url = format!(
                    "{BASE_URL}{APP_PATH}/{TABLE}?vector_attr=embedding&vector_text={}&vector_model=all-MiniLM-L6-v2&limit=10",
                    urlencoded(query)
                );

                let t0 = Instant::now();
                match client.get(&url).send().await {
                    Ok(resp) if resp.status().is_success() => {
                        latencies.push(t0.elapsed().as_secs_f64() * 1000.0);
                        counter.fetch_add(1, Ordering::Relaxed);
                    }
                    _ => {
                        errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            latencies
        });
    }

    let mut all_latencies: Vec<f64> = Vec::new();
    while let Some(Ok(lats)) = tasks.join_next().await {
        all_latencies.extend(lats);
    }

    let elapsed = start.elapsed();
    let total = counter.load(Ordering::Relaxed);
    let err_count = errors.load(Ordering::Relaxed);
    let rps = total as f64 / elapsed.as_secs_f64();

    let (p50, p95, p99) = compute_percentiles(&mut all_latencies);
    let estimate = sampler.finish(rps);

    eprintln!("\n  VECTOR SEARCH RESULTS:");
    eprintln!("  Total queries: {total}");
    eprintln!("  Errors: {err_count}");
    eprintln!("  Duration: {:.2}s", elapsed.as_secs_f64());
    eprintln!("  Throughput: {:.0} query/s", rps);
    eprintln!("  Latency: p50={:.2}ms p95={:.2}ms p99={:.2}ms", p50, p95, p99);
    eprintln!("  Harness CPU: {:.1}% of system", estimate.harness_cpu_fraction * 100.0);
    eprintln!("  Extrapolated: {:.0} query/s", estimate.extrapolated_throughput);

    Ok(TestResult {
        test: "vector-search".to_string(),
        throughput: rps,
        p50,
        p95,
        p99,
        total,
        errors: err_count,
        duration_secs: elapsed.as_secs_f64(),
        harness_overhead: estimate.harness_cpu_fraction,
        extrapolated_throughput: format!("{:.0}", estimate.extrapolated_throughput),
        summary: format!("{:.0} query/s, p50={:.2}ms p99={:.2}ms", rps, p50, p99),
    })
}

/// Simple percent-encoding for query parameter values
fn urlencoded(s: &str) -> String {
    s.replace(' ', "+")
}

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
