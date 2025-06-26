
# Iceberg Metadata Repair Tool

![Apache Iceberg Logo](https://iceberg.apache.org/img/iceberg_logo.png)

**Repairs Apache Iceberg table metadata by cleaning up orphaned and duplicate file references.**

```ascii
╭──────────────────────────╮
│ Iceberg Metadata Repair  │
├──────────────────────────┤
│ 1. Scan Iceberg Metadata │
│ 2. Detect Orphaned Files │
│ 3. Remove Invalid Refs   │
╰──────────────────────────╯
```

## Features
- ✔️ Scans Iceberg table metadata for file references
- ✔️ Detects orphaned files (S3 objects that no longer exist)
- ✔️ Identifies duplicate file references in metadata
- ✔️ Safely removes invalid entries in atomic transactions
- ✔️ Integrates with AWS Glue Catalog and S3

## Prerequisites
- Java 21+
- Apache Iceberg 1.9.1+
- AWS account with necessary permissions
- Maven

## Installation
```bash
git clone https://github.com/your-repo/iceberg-metadata-repair.git
cd iceberg-metadata-repair
mvn clean package
```

## Configuration
### Required IAM Permissions
Your AWS role/profile must have:
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket",
                "s3:GetObject",
                "s3:HeadBucket",
                "s3:HeadObject",
                "glue:GetTable",
                "glue:GetDatabase"
            ],
            "Resource": [
                "arn:aws:s3:::YOUR_BUCKET",
                "arn:aws:s3:::YOUR_BUCKET/*",
                "arn:aws:glue:YOUR_REGION:YOUR_ACCOUNT_ID:catalog",
                "arn:aws:glue:YOUR_REGION:YOUR_ACCOUNT_ID:database/YOUR_DB",
                "arn:aws:glue:YOUR_REGION:YOUR_ACCOUNT_ID:table/YOUR_DB/YOUR_TABLE"
            ]
        }
    ]
}
```

## Usage
```bash
# Using environment variables:
export AWS_REGION=us-east-1
export AWS_PROFILE=my-profile
$(aws configure export-credentials --profile <profile-name> --format env) 

# Using command-line arguments:
java -jar target/iceberg-metadata-repair-1.0.jar \
    <aws-region> \
    <catalog-name> \
    <database-name> \
    <table-name> \
    <bucket-name>
```

### Example Output
```log
SLF4J(W): No SLF4J providers were found.
SLF4J(W): Defaulting to no-operation (NOP) logger implementation
SLF4J(W): See https://www.slf4j.org/codes.html#noProviders for further details.
SLF4J(W): Class path contains SLF4J bindings targeting slf4j-api versions 1.7.x or earlier.
SLF4J(W): Ignoring binding found at [jar:file:/Users/scottcutler/javaProjects/iceberg_missing_file_repair/target/iceberg-metadata-repair-1.0-SNAPSHOT.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J(W): See https://www.slf4j.org/codes.html#ignoredBindings for an explanation.
Authenticated as: ...
<bucket-name> found ✓
Bucket permissions validated ✓
Bucket resides in: us-west-2
Loaded table: rula_securitylake_prod.<table-name>
Found missing URI: s3://<bucket-name>/<table-name>/data/vjpShQ/time_day=2025-06-19/00001-84083-2b07f45c-30fc-4561-92b8-b324d672807c-00002.parquet
Found missing URI: s3://<bucket-name>/<table-name>/data/b_y2Dg/time_day=2025-06-19/00009-84105-27ba87f3-a46f-4050-a455-54e6de4155e7-00002.parquet
Found missing URI: s3://<bucket-name>/<table-name>/data/Ng4AUQ/time_day=2025-06-19/00008-84178-7e6232b1-8c40-4999-8358-a6715fc76211-00002.parquet
Found missing URI: s3://<bucket-name>/<table-name>/data/TDOG2Q/time_day=2025-06-19/00007-84142-839e3f41-cc30-4644-a9a3-8c36992eebbc-00002.parquet
Found missing URI: s3://<bucket-name>/<table-name>/data/rMQ37A/time_day=2025-06-19/00001-84111-8f81503a-6198-45eb-a8e2-e4c98fca1b9d-00002.parquet

```

## How It Works
1. **Initialization**
    - Connects to AWS Glue Catalog
    - Configures Iceberg S3 file IO
    - Loads specified table metadata

2. **File Validation**
   ```mermaid
   graph TD
     A[Scan Iceberg table files] --> B{File exists in S3?}
     B -->|Yes| C[Keep metadata]
     B -->|No| D[Mark for deletion]
     E[Count URI occurrences] --> F{Count > 1?}
     F -->|Yes| D
   ```

3. **Metadata Repair**
    - Uses Iceberg's atomic transactions
    - Applies exponential backoff retries
    - Maintains ACID compliance during cleanup

## Best Practices
- 🚨 **Test First**: Run against a test table before production
- 🕒 **Off-Peak Hours**: Repair large tables during low-traffic periods
- 🔄 **Version Control**: Commit Iceberg metadata before repair
- 🔍 **Review Output**: Always analyze identified files pre-deletion

## Troubleshooting
| Error                    | Solution                                    |
|--------------------------|---------------------------------------------|
| 403 Forbidden            | Verify IAM permissions & bucket ownership  |
| Region Mismatch          | Ensure consistent region across client/S3   |
| NoSuchTableException     | Confirm database/table exists in Glue       |

## Contributing
Pull requests welcome! Please include:
1. Unit tests for new features
2. Updated documentation
3. Integration test cases

## License
Apache 2.0 © 2025 Rula

---


## Author notes:
This README and code was mostly written by an LLM so my apologies in advance. 
Mainly, it really struggles to honor the AWS profile, so instead I used the AWS configure command to populate the environment variables with the necessary creds.
I used it in a pinch when I was getting errors from my Trino and Athena instance like this:
```
ICEBERG_CANNOT_OPEN_SPLIT: Error opening Iceberg split s3://.../data/TFWRPQ/time_day=2025-06-19/00000-84106-586372a6-1969-457c-854c-8c17d411f69f-00002.parquet (offset=4, length=2768278): com.amazonaws.trino.exceptions.UnrecoverableS3OperationException: com.amazonaws.services.s3.model.AmazonS3Exception: The specified key does not exist. (Service: Amazon S3; Status Code: 404; Error Code: NoSuchKey; Request ID: ...

Error opening Iceberg split s3://.../data/xFCOyA/time_day=2025-06-19/00000-84099-2338e6a3-4fbe-401f-9076-cc9efc162383-00002.parquet (offset=4, length=2856644)
```
It appears to be a known issue related to AWS Firehose, see this Slack thread for more details: https://apache-iceberg.slack.com/archives/C025PH0G1D4/p1750792295520389