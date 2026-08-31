# Sample Data
> Last Updated: 22-08-2026 22:33:54

This directory mirrors the S3 object layout used by the local POC.

```text
data/sample/s3/
├── input/
│   ├── json/
│   │   ├── batch-001/
│   │   │   ├── order-0001.json
│   │   │   └── order-0002.json
│   │   └── batch-002/
│   │       ├── order-0003.json
│   │       └── order-0004.json
│   └── lookup/
│       └── customers.csv
└── output/
    └── enriched/
```

The ETL scripts should join JSON orders to `customers.csv` by `customer_id` and write enriched records under `output/enriched`.
