//! Latest Results Resource
//!
//! Returns the most recent benchmark result for each test from the TestRun table.
//!
//! | Method | Path                           | Description                    |
//! |--------|--------------------------------|--------------------------------|
//! | GET    | /benchmarks/best-results        | Latest result per test         |

use yeti_core::prelude::*;

pub type BestResults = BestResultsResource;

#[derive(Default)]
pub struct BestResultsResource;

impl Resource for BestResultsResource {
    fn name(&self) -> &str {
        "best-results"
    }

    get!(_request, ctx, {
        let table = ctx.get_table("TestRun")?;
        let records = table.scan_all().await?;

        // Group by testName, keep only the most recent run per test
        let mut latest: HashMap<String, serde_json::Value> = HashMap::new();
        for record in &records {
            let name = match record.get("testName").and_then(|v| v.as_str()) {
                Some(n) => n.to_string(),
                None => continue,
            };
            let ts = record.get("timestamp").and_then(|v| v.as_str()).unwrap_or("");

            let dominated = latest.get(&name)
                .and_then(|prev| prev.get("timestamp").and_then(|v| v.as_str()))
                .map_or(true, |prev_ts| ts > prev_ts);

            if dominated {
                latest.insert(name, record.clone());
            }
        }

        let mut tests = Vec::new();
        for (name, run) in &latest {
            let results_str = run.get("results").and_then(|v| v.as_str());
            let results: Option<serde_json::Value> = results_str
                .and_then(|s| serde_json::from_str(s).ok());
            let throughput = results.as_ref()
                .and_then(|r| r.get("throughput").and_then(|v| v.as_f64()))
                .unwrap_or(0.0);

            tests.push(json!({
                "name": name,
                "throughput": throughput,
                "run": run,
                "results": results,
            }));
        }

        tests.sort_by(|a, b| {
            let a_name = a["name"].as_str().unwrap_or("");
            let b_name = b["name"].as_str().unwrap_or("");
            a_name.cmp(b_name)
        });

        reply().json(json!({ "tests": tests }))
    });
}

register_resource!(BestResultsResource);
