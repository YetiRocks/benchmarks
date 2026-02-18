//! Common infrastructure for Goose load tests
//!
//! Provides shared utilities for ID generation, record building, configuration,
//! and result collection for output to RESULTS.md.
//!
//! ## Features
//! - ID generation and tracking (RecordTracker)
//! - CRUD helpers (get, post, put, delete)
//! - GraphQL request helpers
//! - Parallel batch seeding for 100K+ records
//! - 150KB HTML blob generation
//! - Results file management

#![allow(dead_code)]

use goose::prelude::*;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::process::Command;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time::{Duration, Instant};

// ============================================================================
// High-Precision Latency Tracking
// ============================================================================
//
// Goose's built-in metrics use integer milliseconds which loses precision for
// sub-millisecond operations. This module provides microsecond-precision timing.

/// Thread-safe latency tracker for multiple operation types
pub struct LatencyTracker {
    /// Latencies stored per operation name (in milliseconds as f64)
    latencies: Mutex<HashMap<String, Vec<f64>>>,
}

impl Default for LatencyTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl LatencyTracker {
    /// Create a new latency tracker
    pub fn new() -> Self {
        Self {
            latencies: Mutex::new(HashMap::new()),
        }
    }

    /// Record a latency measurement for an operation
    pub fn record(&self, operation: &str, latency_ms: f64) {
        if let Ok(mut map) = self.latencies.lock() {
            map.entry(operation.to_string())
                .or_default()
                .push(latency_ms);
        }
    }

    /// Get percentiles (p50, p95, p99) for an operation
    pub fn percentiles(&self, operation: &str) -> (f64, f64, f64) {
        if let Ok(map) = self.latencies.lock() {
            if let Some(times) = map.get(operation) {
                if times.is_empty() {
                    return (0.0, 0.0, 0.0);
                }
                let mut sorted = times.clone();
                sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                let len = sorted.len();
                let p50 = sorted[(len as f64 * 0.50) as usize];
                let p95 = sorted[((len as f64 * 0.95) as usize).min(len - 1)];
                let p99 = sorted[((len as f64 * 0.99) as usize).min(len - 1)];
                (p50, p95, p99)
            } else {
                (0.0, 0.0, 0.0)
            }
        } else {
            (0.0, 0.0, 0.0)
        }
    }

    /// Get count of recorded latencies for an operation
    pub fn count(&self, operation: &str) -> usize {
        if let Ok(map) = self.latencies.lock() {
            map.get(operation).map(|v| v.len()).unwrap_or(0)
        } else {
            0
        }
    }

    /// Get all operation names
    pub fn operations(&self) -> Vec<String> {
        if let Ok(map) = self.latencies.lock() {
            map.keys().cloned().collect()
        } else {
            Vec::new()
        }
    }

    /// Clear all recorded latencies
    pub fn clear(&self) {
        if let Ok(mut map) = self.latencies.lock() {
            map.clear();
        }
    }
}

/// Global latency tracker instance
pub static LATENCY_TRACKER: std::sync::LazyLock<LatencyTracker> =
    std::sync::LazyLock::new(LatencyTracker::new);

/// Record a latency measurement (convenience function)
pub fn record_latency(operation: &str, latency_ms: f64) {
    LATENCY_TRACKER.record(operation, latency_ms);
}

/// Get percentiles for an operation (convenience function)
pub fn get_percentiles(operation: &str) -> (f64, f64, f64) {
    LATENCY_TRACKER.percentiles(operation)
}

/// Clear all latency data (call before each test run)
pub fn reset_latency_tracker() {
    LATENCY_TRACKER.clear();
}

/// Timing guard that automatically records latency on drop
pub struct TimingGuard {
    operation: String,
    start: Instant,
}

impl TimingGuard {
    /// Start timing an operation
    pub fn start(operation: &str) -> Self {
        Self {
            operation: operation.to_string(),
            start: Instant::now(),
        }
    }

    /// Manually record and consume the guard
    pub fn record(self) -> f64 {
        let latency_ms = self.start.elapsed().as_secs_f64() * 1000.0;
        record_latency(&self.operation, latency_ms);
        latency_ms
    }
}

impl Drop for TimingGuard {
    fn drop(&mut self) {
        let latency_ms = self.start.elapsed().as_secs_f64() * 1000.0;
        record_latency(&self.operation, latency_ms);
    }
}

/// Macro for timing a block of code
#[macro_export]
macro_rules! timed {
    ($op:expr, $body:expr) => {{
        let start = std::time::Instant::now();
        let result = $body;
        let latency_ms = start.elapsed().as_secs_f64() * 1000.0;
        $crate::record_latency($op, latency_ms);
        result
    }};
}

// ============================================================================
// Original Infrastructure
// ============================================================================

/// Global atomic counter for unique IDs across all users
pub static GLOBAL_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Reset the global counter (call at start of each test run)
pub fn reset_counter() {
    GLOBAL_COUNTER.store(0, Ordering::SeqCst);
}

// ============================================================================
// Shared Test Configuration
// ============================================================================
// All test parameters are centralized here for consistency across tests.
// Change these values to adjust test behavior globally.

/// Base URL for all load tests
pub const BASE_URL: &str = "https://localhost:9996";

/// Application path for benchmarks app
pub const APP_PATH: &str = "/benchmarks";

/// Default test duration in seconds
pub const TEST_DURATION_SECS: u64 = 10;

/// Warmup request count before main test
pub const WARMUP_REQUESTS: usize = 1000;

/// Concurrent tasks for throughput tests (without Goose)
pub const CONCURRENT_TASKS: usize = 100;

/// Optimal VU count for Goose-based tests
/// 50 VUs with HTTP/2 multiplexing provides good concurrency without saturation
pub const OPTIMAL_VUS: usize = 50;

/// Default iterations for comparison tests
pub const COMPARISON_ITERATIONS: usize = 1000;

/// Profile test duration in seconds (5 minutes)
pub const PROFILE_DURATION_SECS: u64 = 300;

/// Default seed record count for read tests
pub const SEED_RECORD_COUNT: usize = 1_000;

/// Records per batch for seeding
pub const SEED_BATCH_SIZE: usize = 1000;

/// Concurrent batches for parallel seeding
pub const SEED_CONCURRENT_BATCHES: usize = 10;

/// Global flag to track if seeding is complete
pub static SEED_COMPLETE: AtomicBool = AtomicBool::new(false);

/// Size of HTML blob payload in bytes (150KB)
pub const BLOB_PAYLOAD_SIZE: usize = 150 * 1024;

/// Connection pool idle timeout in seconds
pub const POOL_IDLE_TIMEOUT_SECS: u64 = 30;

/// TCP keepalive timeout in seconds
pub const TCP_KEEPALIVE_SECS: u64 = 60;

/// Pre-warm connections by sending parallel requests before the test timer starts.
/// Ensures TLS handshakes and HTTP/2 negotiation don't count against throughput.
pub async fn warmup_connections(client: &reqwest::Client, url: &str, count: usize) {
    use tokio::task::JoinSet;
    let mut tasks = JoinSet::new();
    for _ in 0..count {
        let client = client.clone();
        let url = url.to_string();
        tasks.spawn(async move {
            let _ = client.get(&url).send().await;
        });
        if tasks.len() >= 50 {
            tasks.join_next().await;
        }
    }
    while tasks.join_next().await.is_some() {}
}

/// Signal to the runner that warmup/seeding is done and measurement is starting.
/// Creates a marker file at the path given by YETI_READY_FILE env var.
pub fn signal_ready() {
    if let Ok(path) = std::env::var("YETI_READY_FILE") {
        let _ = std::fs::write(&path, "ready");
        eprintln!("  [ready] Measurement starting");
    }
}

/// Read test duration from YETI_TEST_DURATION env var, with fallback default
pub fn env_duration(default: u64) -> u64 {
    std::env::var("YETI_TEST_DURATION")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

/// Read virtual user count from YETI_TEST_VUS env var, with fallback default
pub fn env_vus(default: usize) -> usize {
    std::env::var("YETI_TEST_VUS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

/// Target read throughput (req/s) for benchmark comparison
pub const TARGET_READ_THROUGHPUT: usize = 150_000;

/// Target write throughput (req/s) for benchmark comparison
pub const TARGET_WRITE_THROUGHPUT: usize = 75_000;

/// Configuration for load tests
#[derive(Clone)]
pub struct LoadConfig {
    pub app_path: String,
    pub table_name: String,
}

impl LoadConfig {
    /// Load configuration from environment variables or defaults
    pub fn from_env() -> Self {
        Self {
            app_path: std::env::var("APP_PATH")
                .unwrap_or_else(|_| "/benchmarks".to_string()),
            table_name: std::env::var("TABLE_NAME")
                .unwrap_or_else(|_| "TableName".to_string()),
        }
    }

    /// Configuration for Harper tests
    pub fn harper() -> Self {
        Self {
            app_path: String::new(), // Harper uses table directly in path
            table_name: std::env::var("TABLE_NAME")
                .unwrap_or_else(|_| "TableName".to_string()),
        }
    }

    /// Build URL for the configured table (Yeti)
    pub fn table_url(&self) -> String {
        format!("{}/{}/", self.app_path, self.table_name)
    }

    /// Build URL for Harper table
    pub fn harper_table_url(&self) -> String {
        format!("/{}/", self.table_name)
    }
}

// ============================================================================
// Record Tracker - Thread-safe ID management for CRUD operations
// ============================================================================

/// Thread-safe tracker for record IDs used in read/update/delete operations.
///
/// Maintains a set of available IDs that can be randomly selected for reads/updates
/// or removed for deletes. Essential for tests that need to operate on existing records.
#[derive(Default)]
pub struct RecordTracker {
    /// Set of available record IDs
    ids: RwLock<Vec<String>>,
    /// Index for round-robin selection
    next_index: AtomicU64,
}

impl RecordTracker {
    /// Create a new empty tracker
    pub fn new() -> Self {
        Self {
            ids: RwLock::new(Vec::new()),
            next_index: AtomicU64::new(0),
        }
    }

    /// Create a tracker with pre-allocated capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            ids: RwLock::new(Vec::with_capacity(capacity)),
            next_index: AtomicU64::new(0),
        }
    }

    /// Add an ID to the tracker (thread-safe)
    pub fn add(&self, id: String) {
        if let Ok(mut ids) = self.ids.write() {
            ids.push(id);
        }
    }

    /// Add multiple IDs (more efficient for batch operations)
    pub fn add_batch(&self, new_ids: Vec<String>) {
        if let Ok(mut ids) = self.ids.write() {
            ids.extend(new_ids);
        }
    }

    /// Get a random ID without removing it (for reads/updates)
    /// Uses round-robin for better distribution under load
    pub fn random_id(&self) -> Option<String> {
        if let Ok(ids) = self.ids.read() {
            if ids.is_empty() {
                return None;
            }
            let index = self.next_index.fetch_add(1, Ordering::Relaxed) as usize % ids.len();
            Some(ids[index].clone())
        } else {
            None
        }
    }

    /// Take a random ID and remove it (for deletes)
    /// Returns None if no IDs available
    pub fn take_random_id(&self) -> Option<String> {
        if let Ok(mut ids) = self.ids.write() {
            if ids.is_empty() {
                return None;
            }
            // Remove from the end for efficiency (O(1) removal)
            ids.pop()
        } else {
            None
        }
    }

    /// Get current count of tracked IDs
    pub fn count(&self) -> usize {
        self.ids.read().map(|ids| ids.len()).unwrap_or(0)
    }

    /// Check if tracker has any IDs
    pub fn is_empty(&self) -> bool {
        self.count() == 0
    }

    /// Clear all tracked IDs
    pub fn clear(&self) {
        if let Ok(mut ids) = self.ids.write() {
            ids.clear();
        }
    }
}

/// Results file path (relative to tests/ directory)
pub const RESULTS_FILE: &str = "../RESULTS.md";

/// Initialize RESULTS.md with header (overwrites existing file)
pub fn init_results_file() {
    let header = format!(
        "# Yeti Load Test Results\n\n\
         **Test Date**: {}\n\
         **Load Tester**: Goose (Rust-based)\n\n\
         ---\n\n",
        chrono::Local::now().format("%Y-%m-%d %H:%M")
    );
    fs::write(RESULTS_FILE, header).expect("Failed to write results file");
}

/// Append a new section to RESULTS.md (preserves existing content)
/// Use this when adding results to an existing file without overwriting previous sections
pub fn append_section_to_results(section_title: &str) {
    if !results_file_exists() {
        init_results_file();
    }
    append_to_results(&format!("## {section_title}\n"));
}

/// Append content to RESULTS.md
pub fn append_to_results(content: &str) {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(RESULTS_FILE)
        .expect("Failed to open results file");
    writeln!(file, "{content}").expect("Failed to write to results file");
}

/// Check if RESULTS.md exists
pub fn results_file_exists() -> bool {
    Path::new(RESULTS_FILE).exists()
}

/// Replace or append a section in RESULTS.md
/// If the section exists, replaces it. Otherwise, appends it.
pub fn update_section_in_results(section_title: &str, content: &str) {
    if !results_file_exists() {
        init_results_file();
    }

    let file_content = fs::read_to_string(RESULTS_FILE)
        .expect("Failed to read results file");

    let section_marker = format!("## {section_title}");

    // Find if section exists
    if let Some(section_start) = file_content.find(&section_marker) {
        // Find the next section (starts with ##) or end of file
        let after_section = &file_content[section_start + section_marker.len()..];
        let section_end = if let Some(next_section_pos) = after_section.find("\n## ") {
            section_start + section_marker.len() + next_section_pos
        } else {
            file_content.len()
        };

        // Replace the section
        let mut new_content = String::new();
        new_content.push_str(&file_content[..section_start]);
        new_content.push_str(&section_marker);
        new_content.push('\n');
        new_content.push_str(content);
        if section_end < file_content.len() {
            new_content.push_str(&file_content[section_end..]);
        }

        fs::write(RESULTS_FILE, new_content)
            .expect("Failed to write results file");
    } else {
        // Section doesn't exist, append it
        append_to_results(&section_marker);
        append_to_results(content);
    }
}

/// Generate a unique ID using atomic counter and user index
pub fn generate_unique_id(user: &GooseUser) -> String {
    let counter = GLOBAL_COUNTER.fetch_add(1, Ordering::SeqCst);
    let user_id = user.weighted_users_index;
    format!("rec_{user_id}_{counter}")
}

/// Build a ~1KB `TableName` record with unique data
pub fn build_bench_record(user: &GooseUser) -> serde_json::Value {
    let counter = GLOBAL_COUNTER.load(Ordering::SeqCst);
    let user_id = user.weighted_users_index;
    let unique_id = generate_unique_id(user);

    // Padding to reach ~1KB record size
    let payload_padding = "X".repeat(550);

    serde_json::json!({
        "id": unique_id,
        "name": format!("User Name {}_{}", user_id, counter),
        "email": format!("user{}_{}@example.com", user_id, counter),
        "category": format!("category_{}", user_id % 10),
        "description": format!("This is a detailed description for record number {}. It contains additional context and information that might be relevant for the application use case.", counter),
        "metadata": format!("{{\"created\":\"{}\",\"version\":\"1.0\",\"tags\":[\"tag1\",\"tag2\",\"tag3\"],\"attributes\":{{\"key1\":\"value1\",\"key2\":\"value2\"}}}}", chrono::Utc::now().timestamp_millis()),
        "payload": payload_padding
    })
}

/// Build an update record (partial update for PUT/PATCH)
pub fn build_update_record(id: &str, user: &GooseUser) -> serde_json::Value {
    let counter = GLOBAL_COUNTER.load(Ordering::SeqCst);
    let user_id = user.weighted_users_index;

    serde_json::json!({
        "id": id,
        "name": format!("Updated Name {}_{}", user_id, counter),
        "description": format!("Updated description at {} by user {}", chrono::Utc::now().timestamp_millis(), user_id),
    })
}

/// Build a GraphQL mutation input from a record
pub fn build_graphql_input(record: &serde_json::Value) -> serde_json::Value {
    // Clone the record and remove any null values for GraphQL
    record.clone()
}

/// Generate a unique ID for seeding (without GooseUser context)
pub fn generate_seed_id(index: usize) -> String {
    format!("seed_{index}")
}

/// Build a record for seeding (without GooseUser context)
pub fn build_seed_record(index: usize) -> serde_json::Value {
    let id = generate_seed_id(index);
    let payload_padding = "X".repeat(550);

    serde_json::json!({
        "id": id,
        "name": format!("Seed User {}", index),
        "email": format!("seed{}@example.com", index),
        "category": format!("category_{}", index % 10),
        "description": format!("Seeded record number {}. This record was created during test setup.", index),
        "metadata": format!("{{\"created\":\"{}\",\"version\":\"1.0\",\"seeded\":true}}", chrono::Utc::now().timestamp_millis()),
        "payload": payload_padding
    })
}

// ============================================================================
// Blob Storage Helpers (150KB HTML payloads)
// ============================================================================

/// Generate a realistic 150KB HTML page for blob storage tests
pub fn generate_html_payload() -> String {
    let mut html = String::with_capacity(BLOB_PAYLOAD_SIZE + 1024);

    html.push_str("<!DOCTYPE html>\n<html lang=\"en\">\n<head>\n");
    html.push_str("<meta charset=\"UTF-8\">\n");
    html.push_str("<meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">\n");
    html.push_str("<title>Load Test Page</title>\n");
    html.push_str("<style>\n");

    // Add CSS (~20KB)
    for i in 0..500 {
        html.push_str(&format!(
            ".class-{i} {{ color: #{:06x}; margin: {margin}px; padding: {padding}px; }}\n",
            i * 123 % 0xFFFFFF,
            margin = i % 20,
            padding = i % 15
        ));
    }

    html.push_str("</style>\n</head>\n<body>\n");
    html.push_str("<header><h1>Load Test Document</h1></header>\n");
    html.push_str("<main>\n");

    // Add HTML content (~120KB) - structured content for realism
    for i in 0..1500 {
        html.push_str(&format!(
            "<article class=\"class-{class}\">\n\
             <h2>Section {i}</h2>\n\
             <p>Lorem ipsum dolor sit amet, consectetur adipiscing elit. \
             Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. \
             Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris.</p>\n\
             <ul>\n\
             <li>Item {a}</li>\n\
             <li>Item {b}</li>\n\
             <li>Item {c}</li>\n\
             </ul>\n\
             <p>Duis aute irure dolor in reprehenderit in voluptate velit esse \
             cillum dolore eu fugiat nulla pariatur.</p>\n\
             </article>\n",
            class = i % 500,
            a = i * 3,
            b = i * 3 + 1,
            c = i * 3 + 2
        ));
    }

    html.push_str("</main>\n");
    html.push_str("<footer><p>Generated for load testing</p></footer>\n");
    html.push_str("</body>\n</html>");

    // Ensure we hit the target size (pad if necessary)
    while html.len() < BLOB_PAYLOAD_SIZE {
        html.push_str("<!-- padding -->\n");
    }

    html
}

/// Build a blob record with 150KB HTML content
pub fn build_blob_record(user: &GooseUser) -> serde_json::Value {
    let id = generate_unique_id(user);
    let html = generate_html_payload();

    serde_json::json!({
        "id": id,
        "title": format!("Page {}", id),
        "content": html,
        "created_at": chrono::Utc::now().to_rfc3339()
    })
}

/// Build a blob record for seeding (without GooseUser context)
pub fn build_seed_blob_record(index: usize) -> serde_json::Value {
    let id = format!("blob_{index}");
    let html = generate_html_payload();

    serde_json::json!({
        "id": id,
        "title": format!("Seeded Page {}", index),
        "content": html,
        "created_at": chrono::Utc::now().to_rfc3339()
    })
}

// ============================================================================
// Relationship Test Data Builders
// ============================================================================

/// Build a Manufacturer record (base level)
pub fn build_manufacturer_record(index: usize) -> serde_json::Value {
    const COUNTRIES: [&str; 5] = ["USA", "Germany", "Japan", "China", "UK"];
    let country = COUNTRIES[index % 5];
    serde_json::json!({
        "id": format!("mfg_{index}"),
        "name": format!("Manufacturer {}", index),
        "country": country
    })
}

/// Build a Brand record (middle level, references Manufacturer)
pub fn build_brand_record(index: usize, manufacturer_count: usize) -> serde_json::Value {
    const COUNTRIES: [&str; 5] = ["USA", "Germany", "Japan", "China", "UK"];
    let mfg_index = index % manufacturer_count;
    let country = COUNTRIES[index % 5];
    serde_json::json!({
        "id": format!("brand_{index}"),
        "name": format!("Brand {}", index),
        "country": country,
        "manufacturerId": format!("mfg_{mfg_index}")
    })
}

/// Build a Product record (top level, references Brand)
pub fn build_product_record(index: usize, brand_count: usize) -> serde_json::Value {
    let brand_index = index % brand_count;
    serde_json::json!({
        "id": format!("prod_{index}"),
        "name": format!("Product {}", index),
        "brandId": format!("brand_{brand_index}"),
        "price": (index % 1000) as f64 + 9.99,
        "inStock": index % 3 != 0
    })
}

// ============================================================================
// HTTP Request Helpers
// ============================================================================

/// POST a JSON record to the specified URL
pub async fn post_json_record(
    user: &mut GooseUser,
    url: &str,
    record: serde_json::Value,
) -> TransactionResult {
    let request_builder = user
        .get_request_builder(&GooseMethod::Post, url)?
        .header("Content-Type", "application/json")
        .body(record.to_string());

    let goose_request = GooseRequest::builder()
        .set_request_builder(request_builder)
        .build();

    user.request(goose_request).await?;

    Ok(())
}

/// GET a single record by URL
pub async fn get_record(user: &mut GooseUser, url: &str) -> TransactionResult {
    let request_builder = user
        .get_request_builder(&GooseMethod::Get, url)?
        .header("Accept", "application/json");

    let goose_request = GooseRequest::builder()
        .set_request_builder(request_builder)
        .build();

    user.request(goose_request).await?;

    Ok(())
}

/// PUT (update) a JSON record at the specified URL
pub async fn put_json_record(
    user: &mut GooseUser,
    url: &str,
    record: serde_json::Value,
) -> TransactionResult {
    let request_builder = user
        .get_request_builder(&GooseMethod::Put, url)?
        .header("Content-Type", "application/json")
        .body(record.to_string());

    let goose_request = GooseRequest::builder()
        .set_request_builder(request_builder)
        .build();

    user.request(goose_request).await?;

    Ok(())
}

/// PATCH (partial update) a JSON record at the specified URL
pub async fn patch_json_record(
    user: &mut GooseUser,
    url: &str,
    record: serde_json::Value,
) -> TransactionResult {
    let request_builder = user
        .get_request_builder(&GooseMethod::Patch, url)?
        .header("Content-Type", "application/json")
        .body(record.to_string());

    let goose_request = GooseRequest::builder()
        .set_request_builder(request_builder)
        .build();

    user.request(goose_request).await?;

    Ok(())
}

/// DELETE a record at the specified URL
pub async fn delete_record(user: &mut GooseUser, url: &str) -> TransactionResult {
    let request_builder = user.get_request_builder(&GooseMethod::Delete, url)?;

    let goose_request = GooseRequest::builder()
        .set_request_builder(request_builder)
        .build();

    user.request(goose_request).await?;

    Ok(())
}

// ============================================================================
// GraphQL Helpers
// ============================================================================

/// Execute a GraphQL query/mutation
pub async fn graphql_request(
    user: &mut GooseUser,
    endpoint: &str,
    query: &str,
    variables: Option<serde_json::Value>,
) -> TransactionResult {
    let body = serde_json::json!({
        "query": query,
        "variables": variables.unwrap_or(serde_json::json!({}))
    });

    let request_builder = user
        .get_request_builder(&GooseMethod::Post, endpoint)?
        .header("Content-Type", "application/json")
        .body(body.to_string());

    let goose_request = GooseRequest::builder()
        .set_request_builder(request_builder)
        .build();

    user.request(goose_request).await?;

    Ok(())
}

/// Reset `TableName` by dropping database and restarting yeti-core
///
/// This ensures each test starts with an empty table for accurate benchmarking.
pub fn reset_bench_table() {
    eprintln!("Resetting TableName...");

    // Get yeti root directory from environment or use default
    let yeti_root = std::env::var("YETI_ROOT")
        .or_else(|_| std::env::var("HOME").map(|h| format!("{h}/yeti")))
        .unwrap_or_else(|_| "/Users/jrepp/yeti".to_string());

    // Path to the TableName database
    let db_path = Path::new(&yeti_root)
        .join("data")
        .join("customers")
        .join("local")
        .join("TableName.mdb");

    // Stop yeti-core if running (force kill to ensure clean shutdown)
    eprintln!("  Stopping yeti-core...");
    let _ = Command::new("pkill")
        .args(["-9", "yeti-core"])
        .output();
    thread::sleep(Duration::from_secs(2));

    // Delete the TableName database directory
    if db_path.exists() {
        eprintln!("  Deleting database: {db_path:?}");
        if let Err(e) = fs::remove_dir_all(&db_path) {
            eprintln!("  Warning: Failed to delete database: {e}");
        }
    }

    // Start yeti-core from the project directory
    eprintln!("  Starting yeti-core...");
    let yeti_exe = std::env::var("YETI_EXE")
        .unwrap_or_else(|_| "./target/release/yeti-core".to_string());

    // Get the project directory (parent of load/)
    let project_dir = std::env::current_dir()
        .expect("Failed to get current directory");

    // Note: yeti-core now reads rootDirectory from yeti-config.yaml
    // No need to pass ROOT_DIRECTORY environment variable
    // We intentionally spawn the process in the background without waiting
    #[allow(clippy::zombie_processes)]
    let _ = Command::new(&yeti_exe)
        .current_dir(&project_dir)
        .spawn()
        .expect("Failed to start yeti-core");

    // Wait for yeti-core to start and recreate the table
    // Need to wait longer for application compilation (can take 10-15 seconds)
    eprintln!("  Waiting for yeti-core to start...");
    thread::sleep(Duration::from_secs(20));

    eprintln!("  TableName reset complete");
}

// ============================================================================
// Seeding Infrastructure
// ============================================================================

/// Configuration for seeding operations
#[derive(Clone)]
pub struct SeedConfig {
    pub base_url: String,
    pub table_name: String,
    pub record_count: usize,
    pub batch_size: usize,
    pub concurrent_batches: usize,
}

impl Default for SeedConfig {
    fn default() -> Self {
        Self {
            base_url: "https://localhost:9996".to_string(),
            table_name: "TableName".to_string(),
            record_count: SEED_RECORD_COUNT,
            batch_size: SEED_BATCH_SIZE,
            concurrent_batches: SEED_CONCURRENT_BATCHES,
        }
    }
}

impl SeedConfig {
    /// Create a seed config for a specific application and table
    pub fn for_app(app_path: &str, table_name: &str) -> Self {
        Self {
            base_url: "https://localhost:9996".to_string(),
            table_name: format!("{}/{}", app_path, table_name),
            ..Default::default()
        }
    }

    /// Set the number of records to seed
    pub fn with_count(mut self, count: usize) -> Self {
        self.record_count = count;
        self
    }
}

/// Result of a seeding operation
#[derive(Debug)]
pub struct SeedResult {
    pub records_seeded: usize,
    pub duration: Duration,
    pub records_per_second: f64,
}

/// Seed records using parallel HTTP batch uploads
///
/// Uses tokio for async parallelism. Must be called from an async context.
/// Returns the seeding result with timing information.
///
/// # Example
/// ```ignore
/// let config = SeedConfig::for_app("/application-template", "TableName")
///     .with_count(100_000);
/// let result = seed_records_parallel(&config, build_seed_record).await?;
/// println!("Seeded {} records in {:?}", result.records_seeded, result.duration);
/// ```
pub async fn seed_records_parallel<F>(
    config: &SeedConfig,
    record_builder: F,
) -> Result<SeedResult, Box<dyn std::error::Error + Send + Sync>>
where
    F: Fn(usize) -> serde_json::Value + Send + Sync + 'static,
{
    use std::sync::Arc;
    use tokio::sync::Semaphore;

    let start = Instant::now();
    let record_builder = Arc::new(record_builder);

    // Build HTTP client with TLS support and timeouts to prevent hangs
    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true) // Load tests use self-signed certs
        .timeout(Duration::from_secs(30))
        .connect_timeout(Duration::from_secs(5))
        .build()?;
    let client = Arc::new(client);

    // Create batches
    let batch_count = (config.record_count + config.batch_size - 1) / config.batch_size;
    let semaphore = Arc::new(Semaphore::new(config.concurrent_batches));

    let mut handles = Vec::with_capacity(batch_count);

    for batch_idx in 0..batch_count {
        let start_idx = batch_idx * config.batch_size;
        let end_idx = std::cmp::min(start_idx + config.batch_size, config.record_count);

        let permit = semaphore.clone().acquire_owned().await?;
        let client = Arc::clone(&client);
        let url = format!("{}/{}", config.base_url, config.table_name);
        let record_builder = Arc::clone(&record_builder);

        let handle = tokio::spawn(async move {
            let mut success_count = 0;

            for idx in start_idx..end_idx {
                let record = record_builder(idx);
                let resp = client
                    .post(&url)
                    .header("Content-Type", "application/json")
                    .body(record.to_string())
                    .send()
                    .await;

                if resp.is_ok() {
                    success_count += 1;
                }
            }

            drop(permit);
            success_count
        });

        handles.push(handle);
    }

    // Wait for all batches and count successes
    let mut total_seeded = 0;
    for handle in handles {
        if let Ok(count) = handle.await {
            total_seeded += count;
        }
    }

    let duration = start.elapsed();
    let records_per_second = total_seeded as f64 / duration.as_secs_f64();

    Ok(SeedResult {
        records_seeded: total_seeded,
        duration,
        records_per_second,
    })
}

/// Seed relationship test data (Manufacturer → Brand → Product hierarchy)
///
/// Creates:
/// - 10 Manufacturers
/// - 100 Brands (each references a Manufacturer)
/// - 1,000 Products (each references a Brand)
pub async fn seed_relationship_data(
    base_url: &str,
    app_path: &str,
) -> Result<SeedResult, Box<dyn std::error::Error + Send + Sync>> {
    let start = Instant::now();
    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .timeout(Duration::from_secs(30))
        .connect_timeout(Duration::from_secs(5))
        .build()?;

    let mut total_seeded = 0;

    // Seed Manufacturers (10)
    eprintln!("  Seeding 10 manufacturers...");
    for i in 0..10 {
        let url = format!("{}{}/Manufacturer", base_url, app_path);
        let record = build_manufacturer_record(i);
        if client
            .post(&url)
            .header("Content-Type", "application/json")
            .body(record.to_string())
            .send()
            .await
            .is_ok()
        {
            total_seeded += 1;
        }
    }

    // Seed Brands (100)
    eprintln!("  Seeding 100 brands...");
    for i in 0..100 {
        let url = format!("{}{}/Brand", base_url, app_path);
        let record = build_brand_record(i, 10);
        if client
            .post(&url)
            .header("Content-Type", "application/json")
            .body(record.to_string())
            .send()
            .await
            .is_ok()
        {
            total_seeded += 1;
        }
    }

    // Seed Products (1,000)
    eprintln!("  Seeding 1,000 products...");
    for i in 0..1000 {
        let url = format!("{}{}/Product", base_url, app_path);
        let record = build_product_record(i, 100);
        if client
            .post(&url)
            .header("Content-Type", "application/json")
            .body(record.to_string())
            .send()
            .await
            .is_ok()
        {
            total_seeded += 1;
        }
    }

    let duration = start.elapsed();
    let records_per_second = total_seeded as f64 / duration.as_secs_f64();

    Ok(SeedResult {
        records_seeded: total_seeded,
        duration,
        records_per_second,
    })
}

/// Wait for seeding to complete (for use in Goose transactions)
///
/// Used in multi-VU tests where only one VU seeds and others wait.
pub async fn wait_for_seed_complete(timeout: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if SEED_COMPLETE.load(Ordering::SeqCst) {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
}

/// Mark seeding as complete
pub fn mark_seed_complete() {
    SEED_COMPLETE.store(true, Ordering::SeqCst);
}

/// Reset the seed complete flag (for test reruns)
pub fn reset_seed_flag() {
    SEED_COMPLETE.store(false, Ordering::SeqCst);
}

// ============================================================================
// CLI Argument Parsing Helpers
// ============================================================================

/// Workload type for REST CRUD tests
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CrudWorkload {
    /// 80% read, 20% write
    ReadHeavy,
    /// 20% read, 80% write
    WriteHeavy,
    /// Equal distribution
    Balanced,
}

impl CrudWorkload {
    /// Parse from string argument
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "read-heavy" | "read" => Some(Self::ReadHeavy),
            "write-heavy" | "write" => Some(Self::WriteHeavy),
            "balanced" | "equal" => Some(Self::Balanced),
            _ => None,
        }
    }

    /// Get transaction weights for this workload
    /// Returns (get_weight, post_weight, put_weight, delete_weight)
    pub fn weights(&self) -> (usize, usize, usize, usize) {
        match self {
            Self::ReadHeavy => (8, 1, 1, 0),
            Self::WriteHeavy => (2, 4, 3, 1),
            Self::Balanced => (3, 3, 2, 2),
        }
    }
}

/// Workload type for relationship tests
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RelationshipWorkload {
    /// No joins (baseline)
    Simple,
    /// 60% no join, 35% single, 5% deep
    Typical,
    /// 20% no join, 50% single, 30% deep
    JoinHeavy,
}

impl RelationshipWorkload {
    /// Parse from string argument
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "simple" | "baseline" => Some(Self::Simple),
            "typical" | "normal" => Some(Self::Typical),
            "join-heavy" | "heavy" => Some(Self::JoinHeavy),
            _ => None,
        }
    }

    /// Get transaction weights for this workload
    /// Returns (no_join_weight, single_join_weight, deep_join_weight)
    pub fn weights(&self) -> (usize, usize, usize) {
        match self {
            Self::Simple => (10, 0, 0),
            Self::Typical => (6, 4, 1),
            Self::JoinHeavy => (2, 5, 3),
        }
    }
}

/// Workload type for GraphQL tests
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum GraphqlWorkload {
    /// 90% queries, 10% mutations
    QueryHeavy,
    /// 20% queries, 80% mutations
    MutationHeavy,
    /// 50% queries, 50% mutations
    Balanced,
}

impl GraphqlWorkload {
    /// Parse from string argument
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "query-heavy" | "query" => Some(Self::QueryHeavy),
            "mutation-heavy" | "mutation" => Some(Self::MutationHeavy),
            "balanced" | "equal" => Some(Self::Balanced),
            _ => None,
        }
    }

    /// Get transaction weights for this workload
    /// Returns (query_weight, mutation_weight)
    pub fn weights(&self) -> (usize, usize) {
        match self {
            Self::QueryHeavy => (9, 1),
            Self::MutationHeavy => (2, 8),
            Self::Balanced => (5, 5),
        }
    }
}

// ============================================================================
// Harness Overhead Estimation
// ============================================================================

/// Result of harness overhead measurement
#[derive(Debug, Clone, serde::Serialize)]
pub struct HarnessEstimate {
    /// Fraction of total system CPU used by the test harness (0.0 - 1.0)
    pub harness_cpu_fraction: f64,
    /// Number of CPU cores on the system
    pub cpu_count: usize,
    /// Raw measured throughput
    pub raw_throughput: f64,
    /// Extrapolated throughput if harness ran on separate machine
    pub extrapolated_throughput: f64,
}

/// Measure harness CPU overhead by sampling process CPU via `ps`
///
/// Spawns a background thread that samples CPU usage every 500ms.
/// Returns a handle that can be stopped to get the average.
pub struct HarnessSampler {
    stop_flag: Arc<AtomicBool>,
    samples: Arc<Mutex<Vec<f64>>>,
    handle: Option<thread::JoinHandle<()>>,
}

impl HarnessSampler {
    /// Start sampling CPU usage of the current process
    pub fn start() -> Self {
        let stop_flag = Arc::new(AtomicBool::new(false));
        let samples = Arc::new(Mutex::new(Vec::new()));
        let pid = std::process::id();

        let flag = stop_flag.clone();
        let samps = samples.clone();

        let handle = thread::spawn(move || {
            while !flag.load(Ordering::Relaxed) {
                // Use `ps` to get CPU% for our process
                if let Ok(output) = Command::new("ps")
                    .args(["-p", &pid.to_string(), "-o", "%cpu="])
                    .output()
                {
                    if let Ok(text) = String::from_utf8(output.stdout) {
                        if let Ok(cpu) = text.trim().parse::<f64>() {
                            if let Ok(mut s) = samps.lock() {
                                s.push(cpu);
                            }
                        }
                    }
                }
                thread::sleep(Duration::from_millis(500));
            }
        });

        Self {
            stop_flag,
            samples,
            handle: Some(handle),
        }
    }

    /// Stop sampling and compute the estimate
    pub fn finish(mut self, raw_throughput: f64) -> HarnessEstimate {
        self.stop_flag.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }

        let cpu_count = num_cpus::get();
        let avg_cpu = {
            let s = self.samples.lock().unwrap_or_else(|e| e.into_inner());
            if s.is_empty() {
                0.0
            } else {
                s.iter().sum::<f64>() / s.len() as f64
            }
        };

        // avg_cpu is percentage of one core (e.g. 450 = 4.5 cores)
        // Total system capacity = cpu_count * 100
        let harness_cpu_fraction = avg_cpu / (cpu_count as f64 * 100.0);

        // If harness uses X fraction of CPU, then in a separate-machine scenario
        // the server gets that CPU back:
        // extrapolated = raw / (1 - harness_fraction)
        let extrapolated = if harness_cpu_fraction < 0.95 {
            raw_throughput / (1.0 - harness_cpu_fraction)
        } else {
            raw_throughput * 2.0 // Cap at 2x if measurement seems wrong
        };

        HarnessEstimate {
            harness_cpu_fraction,
            cpu_count,
            raw_throughput,
            extrapolated_throughput: extrapolated,
        }
    }
}

// ============================================================================
// Test Result Posting
// ============================================================================

/// JSON result format for all test binaries
#[derive(Debug, Clone, serde::Serialize)]
pub struct TestResult {
    pub test: String,
    pub throughput: f64,
    pub p50: f64,
    pub p95: f64,
    pub p99: f64,
    pub total: usize,
    pub errors: usize,
    #[serde(rename = "durationSecs")]
    pub duration_secs: f64,
    #[serde(rename = "harnessOverhead")]
    pub harness_overhead: f64,
    #[serde(rename = "extrapolatedThroughput")]
    pub extrapolated_throughput: String,
    pub summary: String,
}

impl TestResult {
    /// Write JSON result to YETI_RESULT_FILE (if set) and stdout
    pub fn emit(&self) {
        if let Ok(json) = serde_json::to_string(self) {
            // Write to result file for runner to read (avoids stdout pipe deadlock with Goose)
            if let Ok(path) = std::env::var("YETI_RESULT_FILE") {
                let _ = std::fs::write(&path, &json);
            }
            println!("{}", json);
        }
    }

    /// Post directly to the TestRun table (for CLI runs without the runner)
    pub async fn post_to_test_run(&self) {
        let client = match reqwest::Client::builder()
            .danger_accept_invalid_certs(true)
            .timeout(Duration::from_secs(10))
            .connect_timeout(Duration::from_secs(5))
            .build()
        {
            Ok(c) => c,
            Err(e) => {
                eprintln!("Failed to build HTTP client: {}", e);
                return;
            }
        };

        let id = format!("{}_{}", self.test, chrono::Utc::now().timestamp());
        let record = serde_json::json!({
            "id": id,
            "testName": self.test,
            "timestamp": chrono::Utc::now().to_rfc3339(),
            "durationSecs": self.duration_secs,
            "results": serde_json::to_string(self).unwrap_or_default(),
            "summary": self.summary,
            "harnessOverhead": self.harness_overhead,
            "extrapolatedThroughput": self.extrapolated_throughput,
        });

        let url = format!("{}/benchmarks/TestRun", BASE_URL);
        match client.post(&url)
            .header("Content-Type", "application/json")
            .body(record.to_string())
            .send()
            .await
        {
            Ok(resp) => eprintln!("Posted result to TestRun: {}", resp.status()),
            Err(e) => eprintln!("Failed to post result: {}", e),
        }
    }
}
