# ChronDB SQL Protocol Benchmark

This document describes how to run and interpret the ChronDB SQL protocol benchmark tests.

## About the Benchmark

The benchmark is designed to measure the performance of ChronDB's SQL protocol with large datasets (1GB+).
It tests various SQL operations including:

- SELECT with large data volumes
- SEARCH queries (using WHERE clause with LIKE)
- Queries with INNER JOIN
- Queries with LEFT JOIN
- INSERT operations

## Running the Benchmark

### Locally

To run the benchmark locally, use the provided script:

```bash
./scripts/run_benchmark.sh
```

This will:

1. Generate approximately 1GB of test data
2. Run all benchmark operations
3. Save the results to a timestamped file (`benchmark_results_YYYY-MM-DD_HH-MM-SS.txt`)

### Via GitHub Actions

The benchmark can also be run automatically through GitHub Actions:

1. It runs automatically once a week (Sunday at midnight)
2. It can be manually triggered at any time through the GitHub interface:
   - Go to the "Actions" tab in the repository
   - Select the "SQL Protocol Benchmark" workflow
   - Click "Run workflow"

Results are available as workflow artifacts for 90 days.

## Interpreting the Results

The results file contains detailed information about the performance of each SQL operation:

- **SELECT 1000 records**: Time to fetch 1000 records from the main table
- **SEARCH records**: Time to perform a query with LIKE filter
- **INNER JOIN query**: Time to perform a query with INNER JOIN between tables
- **LEFT JOIN query**: Time to perform a query with LEFT JOIN between tables
- **Average INSERT time**: Average time to insert a complete document

Use these results to:

1. Compare performance between different versions of ChronDB
2. Identify performance bottlenecks in specific operations
3. Validate performance improvements after optimizations

## Important Notes

- The benchmark requires at least 4GB of available memory
- The complete execution may take several minutes
- Results may vary depending on hardware and execution environment
- Benchmark failures do not impact CI/CD workflows (continue-on-error is enabled)
