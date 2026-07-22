# retail_sales_lakehouse_platform

            Source System
                    │
                    ▼
          Bronze Layer (Delta)
          ─────────────────────
          Raw Data
          Audit Columns
          No Business Logic
                      │
                      ▼
        Data Quality Validation
                      │
                      ▼
          Silver Layer (Delta)
          ─────────────────────
          Clean Data
          Standardized
          Business Rules Applied
                      │
                      ▼
            Gold Layer (Delta)
          ─────────────────────
          Aggregated KPIs
          Reporting Tables
                      │
                      ▼
            Power BI Dashboard