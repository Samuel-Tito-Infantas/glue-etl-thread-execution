# glue-etl-thread-execution

Local POC for replaying an AWS Glue-style ETL workflow with Docker, LocalStack S3, a pure Python async script, and an AWS Glue PySpark/Spark script.

## POC Layout

```text
.
├── docker/
│   └── localstack/
│       └── init/
│           └── 01-create-s3-bucket.sh
│   └── glue/
│       └── conf/
│           └── spark-defaults.conf
├── scripts/
│   ├── async_python_etl.py
│   └── spark_etl.py              # pending task
├── data/
│   └── sample/
│       └── s3/
│           └── input/
├── tests/                        # pending validation task
├── docker-compose.yml
└── .env
```

## Env Variables
```bash
# .env
AWS_ACCESS_KEY_ID=test
AWS_SECRET_ACCESS_KEY=test
AWS_DEFAULT_REGION=us-east-1
AWS_ENDPOINT_URL=http://localhost:4566
AWS_ENDPOINT_URL_S3=http://localhost:4566

LOCALSTACK_S3_ENDPOINT=http://localstack:4566
S3_BUCKET=glue-etl-poc
S3_INPUT_PREFIX=input/json
S3_LOOKUP_PREFIX=input/lookup
S3_LOOKUP_KEY=input/lookup/customers.csv
S3_OUTPUT_PREFIX=output/enriched
PYTHON_S3_OUTPUT_PREFIX=output/enriched/python
SPARK_S3_OUTPUT_PREFIX=output/enriched/spark
MAX_S3_CONCURRENCY=16

AWS_GLUE_IMAGE=public.ecr.aws/glue/aws-glue-libs:5
AWS_GLUE_VERSION=5.0
```

## Local Services

Start LocalStack and the AWS Glue runner:

```bash
docker compose up -d
```

LocalStack exposes S3 on `http://localhost:4566`, creates the `glue-etl-poc` bucket during startup, and seeds sample objects from `data/sample/s3`.

The Glue runner uses `public.ecr.aws/glue/aws-glue-libs:5` and talks to LocalStack S3 through `http://localstack:4566` inside the Docker network. Spark S3A defaults are mounted from `docker/glue/conf/spark-defaults.conf`.

Check the bucket:

```bash
docker compose exec localstack awslocal s3 ls
```

Check seeded sample objects:

```bash
docker compose exec localstack awslocal s3 ls s3://glue-etl-poc/input/ --recursive
```

Open a shell in the Glue runner:

```bash
docker compose exec glue bash
```

Stop services:

```bash
docker compose down
```

Remove local service state:

```bash
docker compose down -v
```
