# Iceberg Metadata Repair Tool

A Java application that removes broken file references from Iceberg metadata using AWS Glue Catalog.

## Prerequisites

- Java 21+
- Maven
- AWS credentials configured in `~/.aws/credentials`
- MODIFY THE PATH TO THE S3 BUCKET IN THE SOURCE CODE

## Building

```bash
mvn clean package
```

## Running

```bash
$(aws configure export-credentials --profile <aws-profile> --format env)
java -jar target/iceberg-metadata-repair-1.0-SNAPSHOT.jar <aws-profile> <catalog-name> <database-name> <table-name> <file-uri>
```

## Author notes:
This code was mostly written by an LLM and it has some major issues, but it works.
Mainly, it doesn't appear to honor the AWS profile name, so instead I used the AWS configure command to populate the environment variables with the necessary creds.
I used it in a pinch when I was getting errors from my Trino and Athena instance like this:
```
ICEBERG_CANNOT_OPEN_SPLIT: Error opening Iceberg split s3://.../data/TFWRPQ/time_day=2025-06-19/00000-84106-586372a6-1969-457c-854c-8c17d411f69f-00002.parquet (offset=4, length=2768278): com.amazonaws.trino.exceptions.UnrecoverableS3OperationException: com.amazonaws.services.s3.model.AmazonS3Exception: The specified key does not exist. (Service: Amazon S3; Status Code: 404; Error Code: NoSuchKey; Request ID: ...

Error opening Iceberg split s3://.../data/xFCOyA/time_day=2025-06-19/00000-84099-2338e6a3-4fbe-401f-9076-cc9efc162383-00002.parquet (offset=4, length=2856644)
```
It appears to be a known issue related to AWS Firehose, see this Slack thread for more details: https://apache-iceberg.slack.com/archives/C025PH0G1D4/p1750792295520389