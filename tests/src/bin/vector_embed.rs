//! Vector Embedding Load Test
//!
//! Measures POST throughput for records that trigger automatic vector embedding
//! via the yeti-vectors extension. Each POST sends a single text document whose
//! `content` field is embedded into a 384-dim vector by all-MiniLM-L6-v2.
//!
//! The server-side micro-batcher automatically collects concurrent POSTs and
//! batch-embeds them for higher throughput — no client-side batching needed.
//!
//! Usage:
//!   cargo run --release --bin load-vector-embed

use load_common::{
    APP_PATH, BASE_URL, CONCURRENT_TASKS,
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

/// Word bank for generating random documents
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

/// Generate a random document with ~50 words of content
fn generate_document(task_id: usize, seq: u64) -> serde_json::Value {
    let mut rng = rand::thread_rng();
    let word_count = 40 + rng.gen_range(0..20);
    let content: String = (0..word_count)
        .map(|_| WORDS[rng.gen_range(0..WORDS.len())])
        .collect::<Vec<_>>()
        .join(" ");

    let title_words: String = (0..5)
        .map(|_| WORDS[rng.gen_range(0..WORDS.len())])
        .collect::<Vec<_>>()
        .join(" ");

    json!({
        "id": format!("vec_{task_id}_{seq}"),
        "title": title_words,
        "content": content,
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let duration = env_duration(10);
    let vus = env_vus(CONCURRENT_TASKS);

    let client = Client::builder()
        .danger_accept_invalid_certs(true)
        .pool_max_idle_per_host(vus)
        .pool_idle_timeout(Duration::from_secs(POOL_IDLE_TIMEOUT_SECS))
        .tcp_keepalive(Duration::from_secs(TCP_KEEPALIVE_SECS))
        .build()?;

    eprintln!("=== Vector Embedding Load Test ===");
    eprintln!("Target: {BASE_URL}{APP_PATH}/{TABLE}");
    eprintln!("Concurrent tasks: {vus}");
    eprintln!("Test duration: {duration}s\n");

    // Warmup: POST a few documents to warm up TLS + embedding model
    eprintln!("--- Warming up (model + connections) ---");
    let warmup_url = format!("{BASE_URL}{APP_PATH}/{TABLE}");
    // Warm connections first
    warmup_connections(&client, &format!("{BASE_URL}{APP_PATH}/{TABLE}?limit=1"), 50).await;
    // Then warm the embedding model with a few actual POSTs
    for i in 0..5 {
        let doc = json!({
            "id": format!("warmup_vec_{i}"),
            "title": "warmup document",
            "content": "This is a warmup document to initialize the embedding model pipeline",
        });
        let _ = client.post(&warmup_url)
            .header("Content-Type", "application/json")
            .json(&doc)
            .send()
            .await;
    }
    eprintln!("  Warmup complete\n");

    signal_ready();

    let result = run_embed_test(&client, duration, vus).await?;
    result.emit();
    result.post_to_test_run().await;

    Ok(())
}

async fn run_embed_test(client: &Client, duration: u64, vus: usize) -> Result<TestResult, Box<dyn std::error::Error>> {
    eprintln!("--- Vector Embedding Throughput Test (single POSTs, server-side batching) ---");

    let sampler = HarnessSampler::start();

    eprintln!("  Running for {duration}s...");
    let counter = Arc::new(AtomicUsize::new(0));
    let errors = Arc::new(AtomicUsize::new(0));
    let start = Instant::now();
    let deadline = start + Duration::from_secs(duration);

    let write_url = format!("{BASE_URL}{APP_PATH}/{TABLE}");

    let mut tasks: JoinSet<Vec<f64>> = JoinSet::new();

    for task_id in 0..vus {
        let client = client.clone();
        let counter = counter.clone();
        let errors = errors.clone();
        let url = write_url.clone();

        tasks.spawn(async move {
            let mut latencies = Vec::with_capacity(10_000);
            let mut seq = 0u64;
            while Instant::now() < deadline {
                let doc = generate_document(task_id, seq);

                let t0 = Instant::now();
                match client
                    .post(&url)
                    .header("Content-Type", "application/json")
                    .json(&doc)
                    .send()
                    .await
                {
                    Ok(resp) if resp.status().is_success() => {
                        latencies.push(t0.elapsed().as_secs_f64() * 1000.0);
                        counter.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(resp) => {
                        if seq < 3 {
                            eprintln!("  [task {task_id}] Error status: {}", resp.status());
                        }
                        errors.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => {
                        if seq < 3 {
                            eprintln!("  [task {task_id}] Request error: {e}");
                        }
                        errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
                seq += 1;
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

    eprintln!("\n  VECTOR EMBED RESULTS:");
    eprintln!("  Total embeddings: {total}");
    eprintln!("  Errors: {err_count}");
    eprintln!("  Duration: {:.2}s", elapsed.as_secs_f64());
    eprintln!("  Throughput: {:.0} embed/s", rps);
    eprintln!("  Latency: p50={:.2}ms p95={:.2}ms p99={:.2}ms", p50, p95, p99);
    eprintln!("  Harness CPU: {:.1}% of system", estimate.harness_cpu_fraction * 100.0);
    eprintln!("  Extrapolated: {:.0} embed/s", estimate.extrapolated_throughput);

    Ok(TestResult {
        test: "vector-embed".to_string(),
        throughput: rps,
        p50,
        p95,
        p99,
        total,
        errors: err_count,
        duration_secs: elapsed.as_secs_f64(),
        harness_overhead: estimate.harness_cpu_fraction,
        extrapolated_throughput: format!("{:.0}", estimate.extrapolated_throughput),
        summary: format!("{:.0} embed/s, p50={:.2}ms p99={:.2}ms", rps, p50, p99),
    })
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
