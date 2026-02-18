<p align="center">
  <img src="https://cdn.prod.website-files.com/68e09cef90d613c94c3671c0/697e805a9246c7e090054706_logo_horizontal_grey.png" alt="Yeti" width="200" />
</p>

---

# Load Test Suite

[![Yeti](https://img.shields.io/badge/Yeti-Application-blue)](https://yetirocks.com)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/Rust-Powered-orange)](https://rust-lang.org)

Standalone, portable load testing suite for evaluating Yeti server performance. Rust-based benchmarks covering REST CRUD, GraphQL, relationship traversal, large payloads, and sustained-load profiling.

## Features

- **REST & GraphQL Tests** - Full API coverage for both endpoints
- **Concurrency Tuning** - Find optimal virtual user counts with matrix tests
- **Memory Profiling** - Detect leaks with 5-minute sustained load
- **Relationship Testing** - N+1 query benchmarks with 3-level joins
- **Large Payloads** - 150KB HTML blob storage stress tests
- **Throughput Tests** - Minimal-schema max-throughput benchmarks

## Installation

```bash
# Clone into your Yeti applications folder
cd ~/yeti/applications
git clone https://github.com/yetirocks/load-test.git

# Build the test binaries
cd load-test/tests
cargo build --release
```

The load-test app also registers its tables with Yeti on restart (6 tables for different test scenarios).

## Quick Start

```bash
# Run basic REST CRUD test against local server
./tests/target/release/load-rest-crud --host https://localhost:9996

# Run against any Yeti server
./tests/target/release/load-rest-crud --host https://your-server.com

# Find optimal concurrency
./tests/target/release/load-matrix --host https://localhost:9996
```

## Available Tests

| Binary | Description | Use Case |
|--------|-------------|----------|
| `load-rest-crud` | Full REST lifecycle (GET/POST/PUT/DELETE) | General API performance |
| `load-rest-read-profile` | Sustained read test (5 min) | Memory leak detection |
| `load-rest-relationships` | N+1 query testing with joins | Relationship performance |
| `load-graphql` | GraphQL queries and mutations | GraphQL endpoint performance |
| `load-graphql-profile` | Sustained GraphQL test (5 min) | GraphQL stability |
| `load-blob-storage` | 150KB HTML payload tests | Large payload handling |
| `load-basic` | Basic write comparison test | Quick benchmarking |
| `load-matrix` | VU matrix optimization | Find optimal concurrency |
| `load-profile` | 5-minute sustained load | CPU profiling |

## Usage Examples

### Basic Load Test

```bash
# 100 concurrent users, 10,000 requests
./tests/target/release/load-rest-crud \
  --host https://localhost:9996 \
  --users 100 \
  --requests 10000
```

### Memory Leak Detection

```bash
# Run for 5 minutes with monitoring
./tests/target/release/load-rest-read-profile \
  --host https://localhost:9996 \
  --duration 300
```

### Find Optimal Concurrency

```bash
# Test VU counts: 10, 25, 50, 100, 200
./tests/target/release/load-matrix \
  --host https://localhost:9996 \
  --min-users 10 \
  --max-users 200
```

### GraphQL Load Test

```bash
./tests/target/release/load-graphql \
  --host https://localhost:9996 \
  --users 50 \
  --requests 5000
```

### Relationship Chain Test

```bash
# Tests 3-level joins: Product → Brand → Manufacturer
./tests/target/release/load-rest-relationships \
  --host https://localhost:9996
```

### Blob Storage Test

```bash
# Tests 150KB HTML page inserts and reads
./tests/target/release/load-blob-storage \
  --host https://localhost:9996
```

## Output Metrics

```
=== Load Test Results ===
Total Requests:    10,000
Duration:          12.34s
Throughput:        810.5 req/s
Avg Latency:       1.23ms
P50 Latency:       0.98ms
P95 Latency:       2.45ms
P99 Latency:       5.12ms
Error Rate:        0.00%
```

## Schema (6 Test Tables)

```graphql
type TableName @table(database: "load-test") @export {
    id: ID! @primaryKey
    name: String!
    email: String! @indexed
    category: String!
    description: String!
    metadata: String!
    payload: String!
}

type HtmlPage @table(database: "load-test") @export {
    id: ID! @primaryKey
    title: String!
    content: String!         # 150KB HTML blobs
    created_at: String!
}

type Manufacturer @table(database: "load-test") @export {
    id: ID! @primaryKey
    name: String!
    country: String! @indexed
}

type Brand @table(database: "load-test") @export {
    id: ID! @primaryKey
    name: String!
    country: String! @indexed
    manufacturerId: ID! @indexed
    manufacturer: Manufacturer @relationship(from: "manufacturerId")
}

type Product @table(database: "load-test") @export {
    id: ID! @primaryKey
    name: String!
    brandId: ID! @indexed
    price: Float!
    inStock: Boolean!
    brand: Brand @relationship(from: "brandId")
}

type ThroughputTest @table(database: "load-test") @export {
    id: ID! @primaryKey
    name: String!
    value: Int!
    category: String! @indexed
}
```

### Relationship Chain

```
Manufacturer ──1:N──> Brand ──1:N──> Product
```

## Benchmark Results

See `RESULTS.md` for detailed benchmark data. Recent results (M-series Mac):

| Test | Throughput |
|------|-----------|
| Raw reads | ~77,000 req/s |
| Raw writes | ~35,000 req/s |
| REST CRUD cycle | ~1,600 req/s |
| GraphQL queries | ~390 req/s |
| Blob storage | ~1,800 req/s |

## Project Structure

```
load-test/
├── config.yaml          # Application config (tables for test data)
├── schema.graphql       # 6 test tables with relationships
├── RESULTS.md           # Benchmark results
└── tests/
    ├── Cargo.toml       # Rust dependencies
    └── src/
        ├── bin/
        │   ├── load-rest-crud.rs
        │   ├── load-rest-read-profile.rs
        │   ├── load-rest-relationships.rs
        │   ├── load-graphql.rs
        │   ├── load-graphql-profile.rs
        │   ├── load-blob-storage.rs
        │   ├── load-basic.rs
        │   ├── load-matrix.rs
        │   └── load-profile.rs
        └── lib.rs           # Shared test utilities
```

## Learn More

- [Yeti Documentation](https://yetirocks.com/docs)
- [Performance Tuning](https://yetirocks.com/docs/deployment/performance)
- [Monitoring](https://yetirocks.com/docs/deployment/monitoring)

---

Built with [Yeti](https://yetirocks.com) - The fast, declarative database platform.
