## Device Usage Monthly Table and Audit Retention

### 3rd Lambda — deviceusagemubudatamonthly (aka `TelegenceDeviceUsageMubuMonthly`)
- **Purpose**: Monthly aggregated device usage for performance and historical reporting. This reduces the need to scan detailed daily MUBU feeds for analytics and dashboards.
- **Will it update monthly?**: **Yes.** The rollup is refreshed/inserted at billing close when the monthly “Final Usage” file arrives. Daily MUBU loads keep detailed usage current; the monthly table is maintained for the closed billing period and can be corrected by late-file processing when applicable.
- **How data flows (high-level)**:
  - Daily: MUBU files are ingested and bulk-loaded into `TelegenceDeviceUsageMubuStaging`, then merged into canonical usage tables via database procedures such as `UpdateTelegenceMubuUsageFromStaging`, `UpdateMobilityMubuUsageFromTelegence`, and `UpdateLateMubuUsageFromTelegence`.
  - Monthly: The once-per-month “Final Usage” file is ingested into `TelegenceDeviceFinalUsageStaging`, then applied for its `billingPeriodYear`/`billingPeriodMonth` via `UpdateTelegenceFinalUsageFromStaging`. The monthly aggregated table (`TelegenceDeviceUsageMubuMonthly`/`deviceusagemubudatamonthly`) is maintained in SQL as part of this pipeline.
  - Note: The specific monthly table name is not referenced directly in the Lambda code; it is typically built/maintained by stored procedures or views in the database.

### 4th Lambda — Audit Table Retention (`TelegenceDeviceSyncAudit`)
- **Table**: `TelegenceDeviceSyncAudit`.
- **Retention**: **Old audit records are removed per data‑retention rules.** The purge is enforced in the database (stored procedure/scheduled job) or within the 4th Lambda’s codebase/config. This repo does not contain that purge logic directly.
- **Operational notes**:
  - Purge typically runs on a schedule (e.g., daily) and deletes rows older than the configured retention window.
  - The retention window is driven by configuration (DB setting or Lambda environment). To confirm or change it, check the database for an audit purge procedure/job and the 4th Lambda’s configuration.

### Short Answers
- **Purpose of `deviceusagemubudatamonthly`**: Monthly aggregated usage for performance and historical reporting.
- **Does it update monthly?**: **Yes**—on billing close (monthly Final Usage ingestion), with potential late-file corrections.
- **Audit table retention (`TelegenceDeviceSyncAudit`)**: **Old records are purged per data‑retention policy** via DB job/procedure or 4th Lambda logic.
