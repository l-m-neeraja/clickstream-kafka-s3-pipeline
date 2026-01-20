# Event-Driven Clickstream Ingestion Pipeline



## Overview

This project implements an event-driven data ingestion pipeline for simulated clickstream data using Apache Kafka and AWS S3.

The system demonstrates how real-time events can be produced, streamed, processed, and stored efficiently in a data lake using Parquet format.



The pipeline is fully containerized and can be executed locally using Docker Compose with Kafka and LocalStack.



---



## Architecture



### Data Flow

Clickstream Producer → Kafka Topic → Kafka Consumer → S3 (Parquet Data Lake)



### Components

- Kafka Producer (Python): generates simulated clickstream events

- Kafka Broker: handles event streaming

- Kafka Consumer (Python): validates, transforms, batches events

- AWS S3 (LocalStack): stores partitioned Parquet files



---



## Technology Stack

- Apache Kafka

- Python (confluent-kafka, pandas, pyarrow, boto3)

- Docker \& Docker Compose

- AWS S3 (via LocalStack)

- Pytest



---



## Project Structure

```

src/

├── producer.py

├── consumer.py

├── transformations.py

tests/

├── unit/

└── integration/

Dockerfile.producer

Dockerfile.consumer

docker-compose.yml

requirements.txt

.env.example

README.md

```

---



## How to Run



### Prerequisites

- Docker

- Docker Compose



### Start the Pipeline

Run:
```
docker-compose up --build

```

This command starts:

- Kafka \& Zookeeper

- LocalStack (S3)

- Producer service

- Consumer service



---



## Clickstream Event Example
```
{

&nbsp; "user\_id": "uuid",

&nbsp; "event\_type": "view\_product",

&nbsp; "timestamp": "2026-01-19T10:15:30",

&nbsp; "page\_url": "https://example.com",

&nbsp; "product\_id": 101,

&nbsp; "session\_id": "uuid",

&nbsp; "ip\_address": "192.168.1.1"

}

```

---



## Data Processing



### Transformations

- Required field validation

- Normalization (lowercasing)

- Timestamp parsing

- Ingestion timestamp added



### Batching

- Events are processed in batches

- Kafka offsets are committed only after successful S3 writes






## S3 Data Lake Design

- File format: Apache Parquet

- Partitioning: by ingestion time

```

Example output path:

s3://clickstream-bucket/clickstream/year=2026/month=01/day=19/hour=10/<timestamp>-batch.parquet



This design improves analytical query performance.



```

---

## Configuration

All configuration is handled via environment variables.



.env.example:

KAFKA\_BROKER=kafka:9092

AWS\_ACCESS\_KEY\_ID=test

AWS\_SECRET\_ACCESS\_KEY=test

AWS\_DEFAULT\_REGION=us-east-1

AWS\_ENDPOINT\_URL=http://localstack:4566

S3\_BUCKET=clickstream-bucket



---



## Testing



### Unit Tests

python -m pytest tests/unit



### Integration Tests

python -m pytest tests/integration



---



## Error Handling

- Kafka connection failures handled gracefully

- Invalid messages are logged and skipped

- S3 write failures do not crash the consumer



---



## Conclusion

This project demonstrates a production-style, event-driven ingestion pipeline using Kafka and S3, following data engineering best practices for reliability, scalability, and analytics readiness.



