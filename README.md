# DataSentinel

DataSentinel is a Spark-native framework for **data reconciliation and data quality assertions**
on large-scale datasets.

It is designed for teams that:
- Run PySpark pipelines
- Need system-to-system reconciliation
- Want declarative, YAML-driven data checks
- Prefer lightweight tooling over heavy platforms

## Why DataSentinel?

Most data quality tools are either:
- Too generic
- Too heavy
- Not Spark-first
- Hard to adapt for reconciliation use cases

DataSentinel focuses on **explicit comparisons, deterministic checks,
and clear failure reporting**, while staying close to Spark.

## Key Features
- Spark-native execution
- YAML-based configuration
- Pluggable comparison strategies
- CLI-driven execution
- Designed for large datasets

## Quick Start
```bash
datasentinel datasentinel/config.yaml
```

```python
from datasentinel import run, ComparisonStrategy
```

Environment: `SENTINEL_HOME` sets the base directory for result writes. `SENTINEL_INPUT_HOME` sets the base directory for relative input paths in YAML.

Note: install with `pip install datasentinel`, import as `datasentinel`.
## Upcoming
- More asserts (currently only full recon with outer join strategy is supported)
- More loaders 
- Improved CLI
- Concurrent executors (bsed on depends_on in yaml)
- PASS/FAIL specifiers
