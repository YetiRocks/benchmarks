# Yeti Load Test Results

**Test Date**: 2026-01-28
**Configuration**: `tests/src/lib.rs`

---

## Test Configuration

| Parameter | Value |
|-----------|-------|
| `BASE_URL` | https://localhost:9996 |
| `TEST_DURATION_SECS` | 10 |
| `CONCURRENT_TASKS` | 100 |
| `WARMUP_REQUESTS` | 1,000 |
| `SEED_RECORD_COUNT` | 1,000 |
| `TARGET_READ_THROUGHPUT` | 100,000 |
| `TARGET_WRITE_THROUGHPUT` | 50,000 |

---

## Maximum Throughput Test

`cargo run --release --bin throughput`

| Operation | Throughput | Target | % Achieved | Errors |
|-----------|------------|--------|------------|--------|
| READ (GET by ID) | 77,045 req/s | 100K | 77% | 0 |
| WRITE (POST new) | 35,062 req/s | 50K | 70% | 0 |

---

## REST CRUD Performance

**Workload**: Read-Heavy (80/20) | **Users**: 20 | **Iterations**: 1000 | **Total Requests**: 41139

| Operation | p50 | p95 | p99 | Success |
|-----------|-----|-----|-----|---------|
| GET single | 6.50ms | 28.29ms | 43.48ms | - |
| GET list | 69.86ms | 129.27ms | 163.68ms | - |
| POST create | 16.10ms | 40.34ms | 59.27ms | - |
| PUT update | 15.47ms | 38.76ms | 56.02ms | - |
| DELETE | 0.00ms | 0.00ms | 0.00ms | - |

**Overall Success Rate**: 100.0%
**Duration**: 60.00s


## REST Relationship Performance

`cargo run --release --bin load-rest-relationships`

| Operation | Queries (N+1) | Queries (Batch) | p50 | p99 | req/s |
|-----------|---------------|-----------------|-----|-----|-------|
| Single (no join) | 1 | 1 | 1ms | 8ms | 2,394 |
| Single (1 join) | 2 | 2 | 1ms | 10ms | 1,596 |
| Single (2 joins) | 3 | 3 | 1ms | 12ms | 399 |
| List/20 (no join) | 1 | 1 | 3ms | 15ms | 1,197 |
| List/20 (1 join) | 21 | 2 | 5ms | 19ms | 798 |
| List/20 (2 joins) | 41 | 3 | 5ms | 21ms | 399 |

**Workload**: Typical (60/35/5) | **Users**: 20 | **Total**: 406,924 req | **Throughput**: 6,782 req/s | **Success**: 100%

---

## GraphQL Performance

**Workload**: Query-Heavy (90/10) | **Users**: 20 | **Total Requests**: 9222

| Operation | p50 | p95 | p99 |
|-----------|-----|-----|-----|
| Query (single) | 16.64ms | 32.04ms | 43.51ms |
| Query (list) | 470.75ms | 624.00ms | 696.69ms |
| Mutation (create) | 9.83ms | 23.57ms | 34.29ms |
| Mutation (update) | 9.19ms | 21.59ms | 28.69ms |
| Mutation (delete) | 8.50ms | 19.93ms | 29.85ms |

**Success Rate**: 100.0%


## Blob Storage Performance

`cargo run --release --bin load-blob-storage`

| Operation | Count | p50 | p99 | req/s | Data Rate |
|-----------|-------|-----|-----|-------|-----------|
| GET blob (150KB) | 87,660 | 4ms | 12ms | 1,461 | 214 MB/s |
| POST blob (150KB) | 21,916 | 5ms | 27ms | 365 | 53 MB/s |

**Workload**: Read-Heavy (80/20) | **Users**: 10 | **Total**: 109,576 req | **Success**: 100%

---

## Summary

| Test | Throughput | p50 | p99 |
|------|------------|-----|-----|
| Throughput (read) | 77,045 req/s | <1ms | <5ms |
| Throughput (write) | 35,062 req/s | <1ms | <5ms |
| Relationships | 6,782 req/s | 1-5ms | 8-21ms |
| Blob 150KB | 1,826 req/s | 4-5ms | 12-27ms |
| REST CRUD | 1,607 req/s | 1-12ms | 16-42ms |
| GraphQL | 390 req/s | 3-30ms | 21-64ms |

---

## Bottlenecks

- CPU saturation: client + server use 8+ cores (0% idle)
- TLS overhead: 50% of CPU time in system calls
- Client overhead: test client uses 82% as much CPU as server
- Write contention: throughput drops 17% at 200 concurrent tasks
- GraphQL parsing: ~4x slower than equivalent REST

---

## Benchmark Comparison

| Layer | Benchmark | Load Test | Gap |
|-------|-----------|-----------|-----|
| Layer 6 E2E | 109,800 ops/s | 77,045 req/s | 30% |
| Layer 3 Read | 116,400 ops/s | 77,045 req/s | 34% |
## REST Relationship Join Performance

**Workload**: Typical (60/35/5) | **Users**: 20 | **Total Requests**: 175473

### N+1 Query Impact Analysis

**NOTE**: Yeti currently uses N+1 queries for relationships (no batching implemented).

| Operation | Queries | p50 | p95 | p99 |
|-----------|---------|-----|-----|-----|
| Single (no join) | 1 | 2.53ms | 6.45ms | 10.97ms |
| Single (1 join) | 2 | 2.57ms | 6.54ms | 11.05ms |
| Single (2 joins) | 3 | 2.57ms | 6.99ms | 11.86ms |
| List/20 (no join) | 1 | 7.87ms | 16.13ms | 25.72ms |
| List/20 (1 join) | 21 | 15.99ms | 36.75ms | 57.78ms |
| List/20 (2 joins) | 41 | 16.09ms | 36.80ms | 55.66ms |

### Select Syntax Performance

| Syntax | p50 | Notes |
|--------|-----|-------|
| No select | 2.53ms | Baseline |
| select=* | 0.00ms | All fields |
| select=id,name | 0.00ms | Partial fields |

**Optimization Opportunity**: Batch loading could reduce list queries from 21/41 to 2/3.

**Success Rate**: 100.0%


## Blob Storage Performance (150KB HTML)

**Workload**: Read-Heavy (80/20) | **Payload**: 150KB | **Users**: 10 | **Total Requests**: 14025

| Operation | Count | p50 | p95 | p99 | Throughput |
|-----------|-------|-----|-----|-----|------------|
| Write 150KB | 2805 | 23.95ms | 49.23ms | 68.75ms | 6.85MB/s |
| Read 150KB | 11220 | 43.40ms | 74.17ms | 109.33ms | 27.39MB/s |

**Success Rate**: 100.0%
**Duration**: 60.00s


## REST Update Performance

**Users**: 50 | **Total**: 411517 | **Duration**: 30.0s

| p50 | p99 | Throughput | Success |
|-----|-----|-----------|--------|
| 3.52ms | 7.02ms | 13717 req/s | 100.0% |


## REST Join Performance

**Query**: `Product.name + Brand.name` (1 join) | **Users**: 100 | **Total**: 2417674 | **Duration**: 30.0s

| p50 | p99 | Throughput | Success |
|-----|-----|-----------|--------|
| 1.15ms | 2.79ms | 80589 req/s | 100.0% |


## GraphQL Read Performance

**Users**: 100 | **Total**: 1457030 | **Duration**: 30.0s

| p50 | p99 | Throughput | Success |
|-----|-----|-----------|--------|
| 1.82ms | 5.47ms | 48568 req/s | 100.0% |


## GraphQL Mutation Performance

**Users**: 100 | **Total**: 223108 | **Duration**: 30.0s

| p50 | p99 | Throughput | Success |
|-----|-----|-----------|--------|
| 12.63ms | 28.14ms | 7437 req/s | 100.0% |


## Blob Retrieval Performance

**Payload**: 150KB HTML | **Users**: 50 | **Total**: 70477 | **Duration**: 30.0s

| p50 | p99 | Throughput | MB/s | Success |
|-----|-----|-----------|------|--------|
| 18.90ms | 67.52ms | 2349 req/s | 344.1 | 100.0% |

