//! Test Runner Resource
//!
//! Manages running load test binaries from the UI.
//!
//! | Method | Path                     | Description           |
//! |--------|--------------------------|-----------------------|
//! | GET    | /benchmarks/runner        | Get current status    |
//! | POST   | /benchmarks/runner        | Start a test          |

use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;
use yeti_core::prelude::*;

/// Kill tests that run longer than 3 minutes (covers 60s test + seed + cleanup)
const TEST_TIMEOUT_SECS: u64 = 180;

struct TestDef {
    name: &'static str,
    binary: &'static str,
    args: &'static [&'static str],
    env_vars: &'static [(&'static str, &'static str)],
    default_duration: u64,
    default_vus: u64,
    /// Tables to truncate before running this test
    tables: &'static [&'static str],
}

/// Valid test definitions with default configurations.
/// Note: Test binaries post their own results to TestRun table via post_to_test_run().
/// The runner only captures JSON from stdout for UI display — no HTTP calls from dylib.
const TESTS: &[TestDef] = &[
    TestDef { name: "rest-read", binary: "throughput", args: &["read"], env_vars: &[], default_duration: 30, default_vus: 50, tables: &["ThroughputTest"] },
    TestDef { name: "rest-write", binary: "throughput", args: &["write"], env_vars: &[], default_duration: 30, default_vus: 50, tables: &["ThroughputTest"] },
    TestDef { name: "rest-update", binary: "load-rest-crud", args: &[], env_vars: &[], default_duration: 30, default_vus: 50, tables: &["TableName"] },
    TestDef { name: "rest-join", binary: "load-rest-relationships", args: &[], env_vars: &[], default_duration: 30, default_vus: 50, tables: &["Product", "Brand", "Manufacturer"] },
    TestDef { name: "graphql-read", binary: "load-graphql", args: &[], env_vars: &[("GRAPHQL_MODE", "read")], default_duration: 30, default_vus: 50, tables: &["TableName"] },
    TestDef { name: "graphql-mutation", binary: "load-graphql", args: &[], env_vars: &[("GRAPHQL_MODE", "mutation")], default_duration: 30, default_vus: 50, tables: &["TableName"] },
    TestDef { name: "vector-embed", binary: "load-vector-embed", args: &[], env_vars: &[], default_duration: 30, default_vus: 50, tables: &["VectorDoc"] },
    TestDef { name: "vector-search", binary: "load-vector-search", args: &[], env_vars: &[], default_duration: 30, default_vus: 50, tables: &["VectorDoc"] },
    TestDef { name: "ws", binary: "load-ws", args: &[], env_vars: &[], default_duration: 30, default_vus: 50, tables: &["ThroughputTest"] },
    TestDef { name: "sse", binary: "load-sse", args: &[], env_vars: &[], default_duration: 30, default_vus: 50, tables: &["ThroughputTest"] },
    TestDef { name: "blob-retrieval", binary: "load-blob-storage", args: &[], env_vars: &[], default_duration: 30, default_vus: 50, tables: &["HtmlPage"] },
];

#[derive(Clone)]
struct RunnerState {
    status: String,         // "idle", "warming", or "running"
    test_name: String,
    started_at: u64,        // unix timestamp (when test was initiated)
    started_instant: Option<Instant>,   // when test process started
    running_instant: Option<Instant>,   // when test signaled ready (timed portion)
    configured_duration: u64,           // configured test duration in seconds
    last_result: Option<serde_json::Value>,
    last_error: Option<String>,
}

impl Default for RunnerState {
    fn default() -> Self {
        Self {
            status: "idle".to_string(),
            test_name: String::new(),
            started_at: 0,
            started_instant: None,
            running_instant: None,
            configured_duration: 0,
            last_result: None,
            last_error: None,
        }
    }
}

static STATE: OnceLock<Arc<Mutex<RunnerState>>> = OnceLock::new();

fn state() -> &'static Arc<Mutex<RunnerState>> {
    STATE.get_or_init(|| Arc::new(Mutex::new(RunnerState::default())))
}

fn tests_dir() -> PathBuf {
    let root = std::env::var("ROOT_DIRECTORY").unwrap_or_else(|_| "~/yeti".to_string());
    let root_path = if root.starts_with("~/") {
        if let Some(home) = std::env::var_os("HOME") {
            PathBuf::from(home).join(root.strip_prefix("~/").unwrap())
        } else {
            PathBuf::from(&root)
        }
    } else {
        PathBuf::from(&root)
    };
    root_path.join("applications").join("benchmarks").join("tests")
}

/// Read effective configs: DB overrides merged with defaults
fn read_configs(db_configs: &[serde_json::Value]) -> Vec<serde_json::Value> {
    TESTS.iter().map(|t| {
        let db = db_configs.iter().find(|c| {
            c.get("id").and_then(|v| v.as_str()) == Some(t.name)
        });
        json!({
            "id": t.name,
            "duration": db.and_then(|c| c.get("duration").and_then(|v| v.as_u64())).unwrap_or(t.default_duration),
            "vus": db.and_then(|c| c.get("vus").and_then(|v| v.as_u64())).unwrap_or(t.default_vus),
        })
    }).collect()
}

pub type Runner = RunnerResource;

#[derive(Default)]
pub struct RunnerResource;

impl Resource for RunnerResource {
    fn name(&self) -> &str {
        "runner"
    }

    get!(_request, ctx, {
        // Build response from state — drop lock before any .await
        let mut response = {
            let s = state().lock().map_err(|e| YetiError::Internal(format!("Lock error: {}", e)))?;

            let warmup_secs = match (s.started_instant, s.running_instant) {
                (Some(start), Some(running)) => running.duration_since(start).as_secs_f64(),
                (Some(start), None) if s.status == "warming" => start.elapsed().as_secs_f64(),
                _ => 0.0,
            };

            let elapsed_secs = s.running_instant
                .map(|i| i.elapsed().as_secs_f64())
                .unwrap_or(0.0);

            let mut r = json!({
                "status": s.status,
                "testName": s.test_name,
                "startedAt": s.started_at,
                "warmupSecs": warmup_secs,
                "elapsedSecs": elapsed_secs,
                "configuredDuration": s.configured_duration,
            });

            if let Some(ref result) = s.last_result {
                r["lastResult"] = result.clone();
            }
            if let Some(ref error) = s.last_error {
                r["lastError"] = json!(error);
            }
            r
        }; // MutexGuard dropped here

        // Include effective test configs (defaults merged with DB overrides)
        let db_configs = match ctx.get_table("TestConfig") {
            Ok(table) => table.scan_all().await.unwrap_or_default(),
            Err(_) => vec![],
        };
        response["configs"] = json!(read_configs(&db_configs));

        reply().json(response)
    });

    post!(request, ctx, {
        let body = request.json_value()?;
        let test_name = body.require_str("test")?;

        // Validate test name
        let test_def = TESTS.iter().find(|t| t.name == test_name.as_str());
        let test_def = match test_def {
            Some(def) => def,
            None => {
                let valid: Vec<&str> = TESTS.iter().map(|t| t.name).collect();
                return bad_request(&format!(
                    "Unknown test '{}'. Valid tests: {}",
                    test_name,
                    valid.join(", ")
                ));
            }
        };

        // Check if already running
        {
            let s = state().lock().map_err(|e| YetiError::Internal(format!("Lock error: {}", e)))?;
            if s.status == "running" {
                return bad_request(&format!("Test '{}' is already running", s.test_name));
            }
        }

        // Read config from TestConfig table, falling back to defaults
        let db_configs = match ctx.get_table("TestConfig") {
            Ok(table) => table.scan_all().await.unwrap_or_default(),
            Err(_) => vec![],
        };
        let db_config = db_configs.iter().find(|c| {
            c.get("id").and_then(|v| v.as_str()) == Some(test_name.as_str())
        });
        let duration = db_config
            .and_then(|c| c.get("duration").and_then(|v| v.as_u64()))
            .unwrap_or(test_def.default_duration);
        let vus = db_config
            .and_then(|c| c.get("vus").and_then(|v| v.as_u64()))
            .unwrap_or(test_def.default_vus);

        // Truncate tables from previous run
        for table_name in test_def.tables {
            match ctx.get_table(table_name) {
                Ok(table) => {
                    if let Err(e) = table.backend().truncate().await {
                        eprintln!("[runner] Failed to truncate {}: {}", table_name, e);
                    } else {
                        eprintln!("[runner] Truncated {}", table_name);
                    }
                }
                Err(e) => eprintln!("[runner] Table {} not found: {}", table_name, e),
            }
        }

        // Set state to warming
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // Create ready-file path for the test binary to signal when measurement starts
        let ready_file = std::env::temp_dir().join(format!("yeti-test-ready-{}", test_name));
        let _ = std::fs::remove_file(&ready_file);

        {
            let mut s = state().lock().map_err(|e| YetiError::Internal(format!("Lock error: {}", e)))?;
            s.status = "warming".to_string();
            s.test_name = test_name.to_string();
            s.started_at = now;
            s.started_instant = Some(Instant::now());
            s.running_instant = None;
            s.configured_duration = duration;
            s.last_result = None;
            s.last_error = None;
        }

        // Spawn test in a background thread (NOT tokio::spawn — dylib boundary)
        // IMPORTANT: No reqwest or tokio usage in this thread — crashes in dylib context.
        // Test binaries post their own results to TestRun via post_to_test_run().
        let state_ref = Arc::clone(state());
        let tests_path = tests_dir();
        let bin = test_def.binary.to_string();
        let args: Vec<String> = test_def.args.iter().map(|s| s.to_string()).collect();
        let mut envs: Vec<(String, String)> = test_def.env_vars.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        let test_id = test_name.to_string();
        let ready_path = ready_file.clone();

        // Create result file path for test binary to write JSON result
        let result_file = std::env::temp_dir().join(format!("yeti-test-result-{}", test_name));
        let _ = std::fs::remove_file(&result_file);

        // Pass config as env vars so test binaries use the configured values
        envs.push(("YETI_TEST_DURATION".to_string(), duration.to_string()));
        envs.push(("YETI_TEST_VUS".to_string(), vus.to_string()));
        envs.push(("YETI_READY_FILE".to_string(), ready_file.to_string_lossy().to_string()));
        envs.push(("YETI_RESULT_FILE".to_string(), result_file.to_string_lossy().to_string()));

        std::thread::spawn(move || {
            eprintln!("[runner] Starting test: {} (bin: {}, duration: {}s, vus: {}, dir: {:?})", test_id, bin, duration, vus, tests_path);

            let mut cmd = std::process::Command::new("cargo");
            cmd.args(["run", "--release", "--bin", &bin, "--"]);
            for arg in &args {
                cmd.arg(arg);
            }
            for (key, val) in &envs {
                cmd.env(key, val);
            }
            cmd.current_dir(&tests_path);
            // Don't pipe stdout — Goose writes large reports that fill the pipe buffer
            // and deadlock. Results come via YETI_RESULT_FILE instead.
            cmd.stdout(std::process::Stdio::null());
            cmd.stderr(std::process::Stdio::piped());

            let timeout = std::time::Duration::from_secs(TEST_TIMEOUT_SECS);

            match cmd.spawn() {
                Ok(mut child) => {
                    let pid = child.id();
                    let start = std::time::Instant::now();

                    // Poll for completion with timeout, checking ready file for phase transition
                    let result = loop {
                        match child.try_wait() {
                            Ok(Some(status)) => break Ok(status),
                            Ok(None) => {
                                // Check if test binary signaled ready (warming → running)
                                if ready_path.exists() {
                                    if let Ok(mut s) = state_ref.lock() {
                                        if s.status == "warming" {
                                            s.status = "running".to_string();
                                            s.running_instant = Some(Instant::now());
                                            eprintln!("[runner] Test {} ready, measurement started", test_id);
                                        }
                                    }
                                }

                                if start.elapsed() > timeout {
                                    eprintln!("[runner] Test {} timed out after {}s, killing pid {}", test_id, TEST_TIMEOUT_SECS, pid);
                                    let _ = child.kill();
                                    let _ = child.wait();
                                    break Err("timed out".to_string());
                                }
                                std::thread::sleep(std::time::Duration::from_millis(500));
                            }
                            Err(e) => break Err(format!("wait error: {}", e)),
                        }
                    };

                    // Cleanup ready file
                    let _ = std::fs::remove_file(&ready_path);

                    let stderr = child.stderr.take()
                        .map(|mut s| { let mut buf = String::new(); std::io::Read::read_to_string(&mut s, &mut buf).ok(); buf })
                        .unwrap_or_default();

                    if !stderr.is_empty() {
                        let lines: Vec<&str> = stderr.lines().collect();
                        let start = if lines.len() > 10 { lines.len() - 10 } else { 0 };
                        for line in &lines[start..] {
                            eprintln!("[runner] {}", line);
                        }
                    }

                    // Read result from file (avoids stdout pipe deadlock with Goose)
                    let result_json: Option<serde_json::Value> = std::fs::read_to_string(&result_file)
                        .ok()
                        .and_then(|s| serde_json::from_str(&s).ok());
                    let _ = std::fs::remove_file(&result_file);

                    if let Ok(mut s) = state_ref.lock() {
                        s.status = "idle".to_string();
                        s.last_result = result_json;
                        match result {
                            Ok(status) => {
                                eprintln!("[runner] Test {} finished (exit: {})", test_id, status);
                                if !status.success() {
                                    s.last_error = Some(format!("Exit code: {}", status));
                                }
                            }
                            Err(reason) => {
                                s.last_error = Some(format!("Test killed: {}", reason));
                            }
                        }
                    }
                }
                Err(e) => {
                    eprintln!("[runner] Failed to run test {}: {}", test_id, e);
                    if let Ok(mut s) = state_ref.lock() {
                        s.status = "idle".to_string();
                        s.last_error = Some(format!("Failed to start: {}", e));
                    }
                }
            }
        });

        reply().code(202).json(json!({
            "status": "warming",
            "testName": test_name,
            "startedAt": now,
        }))
    });
}

register_resource!(RunnerResource);
