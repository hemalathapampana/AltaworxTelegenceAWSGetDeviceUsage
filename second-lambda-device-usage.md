### 2nd Lambda — Do we get device usage data here too, or only through MUBU?

**Short answer:** Yes — the 2nd Lambda ingests device usage data from multiple sources, not only MUBU.

- **Premier/Telegence unbilled usage**: Reads latest usage files and loads them into `TelegenceAllUsageStaging`.
- **MUBU (voice and data)**: Downloads MUBU files and loads them into `TelegenceDeviceUsageMubuStaging`, then runs follow-up sync steps.
- **Final usage (billing-cycle close)**: When configured, loads final usage into `TelegenceDeviceFinalUsageStaging`.

#### Where this happens (key spots)
- File: `AltaworxTelegenceAWSGetDeviceUsage.cs`
  - `ProcessDailyUsage(...)` orchestrates all three flows.
  - Premier unbilled usage → `SqlBulkCopy(..., "TelegenceAllUsageStaging")`
  - MUBU usage → `SqlBulkCopy(..., "TelegenceDeviceUsageMubuStaging", ...)`
  - Final usage → `SqlBulkCopy(..., "TelegenceDeviceFinalUsageStaging")`

```csharp
// Premier unbilled usage
SqlBulkCopy(context, context.CentralDbConnectionString, usage, "TelegenceAllUsageStaging");

// MUBU usage
SqlBulkCopy(context, context.CentralDbConnectionString, mubuUsage, "TelegenceDeviceUsageMubuStaging", MubuReportReader.GetRecordColumnMapping());

// Final usage
SqlBulkCopy(context, context.CentralDbConnectionString, finalUsage, "TelegenceDeviceFinalUsageStaging");
```

**Conclusion:** The 2nd Lambda collects device usage data from both Premier/Telegence feeds and MUBU (and optionally final usage), so it is not limited to MUBU.