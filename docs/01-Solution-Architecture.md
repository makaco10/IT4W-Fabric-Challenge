# IT4W Challenge — Solution Architecture

**Project:** Nexo Retail — Data Engineering Challenge  
**Platform:** Microsoft Fabric  
**Architecture:** Medallion with Landing Zone  
**Status:** Initial Architecture Definition

## 1. Objective

Implement a reliable, reproducible and auditable data engineering solution for Nexo Retail, supporting historical ingestion, incremental processing, data quality validation, currency conversion and analytical reporting.

## 2. Technology Stack

| Component | Technology |
|---|---|
| Source | SQL Server 2022 in Docker |
| Connectivity | On-premises Data Gateway |
| Orchestration | Microsoft Fabric Data Pipelines |
| Processing | Python 3.10+ / PySpark |
| Storage | OneLake / Delta Lake |
| Source control | GitHub |
| Exchange rates | Frankfurter API |

The solution will also provide a reproducible execution path using the original challenge files.

## 3. Data Architecture

**Source → Landing → Bronze → Silver → Gold**

- **Landing:** Source extracts stored as Parquet files, separated by source, batch and execution.
- **Bronze:** Delta tables with explicit, version-controlled schemas.
- **Silver:** Data validation, normalization, deduplication and quarantine of invalid records.
- **Gold:** Curated analytical model, including USD and local-currency values.

## 4. Ingestion Strategy

Two independent orchestration pipelines will be implemented:

**PL_IT4W_Backfill_Historico**

Processes historical data for 2024–2025. Executed on demand and designed to support controlled reprocessing.

**PL_IT4W_Ingesta_Periodica**

Processes incremental monthly deliveries, starting with January 2026. Supports scheduling, auditability and idempotent execution.

Both pipelines will use common, reusable processing components where appropriate.

## 5. Data Governance

- Explicit schemas and data types.
- No automatic schema evolution.
- Controlled modifications through versioned contracts.
- Validation of business keys and duplicate records.
- Quarantine with rejection reasons.
- Execution audit logs.
- Configurable Landing retention policies.
- Deletion only after successful processing and the retention period.

## 6. Operational Requirements

The solution must support automated tests, execution metrics, failure handling, alerts and controlled reprocessing.

The final implementation will include the six control queries required by the challenge.

## 7. Architecture Decisions

**ADR-001:** Separate Landing from Bronze to enable reprocessing without accessing the original source.

**ADR-002:** Separate historical backfill from scheduled incremental processing.

**ADR-003:** Use strict, version-controlled data contracts and disable automatic schema evolution.

**ADR-004:** Use GitHub to maintain source code, technical documentation and traceable implementation history.

**ADR-005:** Preserve rejected records and processing evidence for audit and troubleshooting.

## 8. Implementation Status

Initial architecture approved for implementation.

Technical components and processing rules will be implemented and validated incrementally. Changes to these decisions will be recorded in subsequent commits.
