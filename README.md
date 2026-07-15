# Real-Time Flight Data Ingestion with Databricks Auto Loader

An incremental, checkpoint-based data ingestion pipeline built with **Databricks Auto Loader**, processing real U.S. federal airline on-time performance data as it "arrives," with verified data quality checks.

## Overview

Unlike a one-time batch load, this pipeline demonstrates genuine incremental file-based ingestion: as new monthly data files are added to cloud storage, Auto Loader automatically detects and processes only the new files — never reprocessing prior data — using Delta Lake checkpointing.

## Data Source

[Bureau of Transportation Statistics (BTS) — Airline On-Time Performance Data](https://www.transtats.bts.gov/ontime/): official U.S. Department of Transportation data reported monthly by certificated air carriers, covering flight schedules, actual departure/arrival times, delays, and cancellations.

## Architecture

```
Monthly BTS files (added incrementally to a Volume)
        ↓
Auto Loader (cloudFiles) — detects new files automatically
        ↓
Schema inference disabled — explicit, safe type casting
        ↓
Data quality validation (two independent checks)
        ↓
flights (Delta table, incrementally updated)
```

## Verified Incremental Behavior

The pipeline was tested across three real monthly files (January–March), confirming Auto Loader's checkpoint correctly avoided reprocessing:

| Month Added | Row Count Added | Running Total |
|---|---|---|
| January | 539,747 | 539,747 |
| February | 504,884 | 1,044,631 |
| March | 600,872 | 1,645,503 |

Each total was verified arithmetically to confirm no duplication and no data loss occurred between incremental runs.

## Data Quality Checks

1. **Core field completeness**: flags any flight missing `Origin`, `Dest`, or `FlightDate`. Result: 100% complete, consistent with these being federally mandated reporting fields.
2. **Delay cause completeness**: for flights delayed 15+ minutes (`ArrDel15 = 1`), flags any record missing all five delay-cause fields (Carrier, Weather, NAS, Security, Late Aircraft). Result: 100% complete across 317,266 verified delayed flights.

Both results were independently sanity-checked against raw counts (e.g., confirming a meaningful number of delayed flights existed) before being accepted as valid findings, rather than assumed correct from a single pass.

## Key Engineering Decisions

- **Disabled automatic type inference** (`cloudFiles.inferColumnTypes: false`): real-world monthly files from the same source were found to have inconsistent numeric formatting (e.g., `"0"` vs. `"0.00"`) across different months, which caused a type-casting failure under automatic inference. Reading all columns as strings and casting explicitly avoided this class of error entirely.
- **Full pipeline refresh on schema changes**: adding new derived columns to an already-running Auto Loader pipeline does not retroactively apply to already-ingested data, since the checkpoint has no new files to process. Clearing the checkpoint and schema location to force a full reprocessing run is the correct approach when transformation logic changes.

## Tech Stack
`Databricks` · `Auto Loader (cloudFiles)` · `PySpark Structured Streaming` · `Delta Lake` · `Python`

## Future Work
- Add a continuous (rather than `availableNow`) trigger for genuinely ongoing production use
- Build a Silver-layer aggregation (e.g., on-time performance by carrier and route)
- Add schema evolution handling for future files with new or renamed columns
