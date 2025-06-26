package org.rula;

import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.util.Tasks;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.s3.S3Client;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.ContentFile;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.GetCallerIdentityResponse;

import java.net.URI;
import java.util.*;
import java.util.Map;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class IcebergMetadataRepair {

    // S3 validation helper
    private static boolean s3ObjectExists(S3Client s3, String s3uri) {
        try {
            URI uriObj = URI.create(s3uri);
            String key = uriObj.getPath().substring(1); // Remove leading '/'
            HeadObjectRequest request = HeadObjectRequest.builder()
                    .bucket(uriObj.getHost())
                    .key(key)
                    .build();
            s3.headObject(request);
            return true;
        } catch (S3Exception e) {
            if (e.statusCode() == 404) return false;
            throw new RuntimeException("URI: " + s3uri + " S3 error: " + e.getMessage());
        }
    }

    public static void main(String[] args) {
        // Updated argument handling (added region as 1st parameter)
        if (args.length < 5) {
            System.err.println("Usage: IcebergMetadataRepair <aws-region> <catalog-name> <database-name> <table-name> <bucket-name>");
            System.exit(1);
        }

        // Parse region first
        Region awsRegion = Region.of(args[0]); // Convert String → Region
        String catalogName = args[1];
        String databaseName = args[2];
        String tableName = args[3];
        String bucketName = args[4];

        try {
            // Create AWS clients with profile credentials
            S3Client s3 = S3Client.builder()
                    .credentialsProvider(ProfileCredentialsProvider.create())
                    .region(awsRegion)
                    .build();

            // Configure Iceberg catalog
            GlueCatalog catalog = new GlueCatalog();

            // Add these checks after client initialization
            try {
                // 1. Verify STS identity matches expected account
                StsClient sts = StsClient.builder()
                        .credentialsProvider(ProfileCredentialsProvider.create())
                        .region(awsRegion)
                        .build();
                GetCallerIdentityResponse identity = sts.getCallerIdentity();
                System.out.println("Authenticated as: " + identity.arn());

                // 2. Validate AWS credentials are being used
                ListBucketsResponse buckets = s3.listBuckets();
                boolean found = buckets.buckets().stream()
                        .anyMatch(b -> b.name().equals(bucketName));
                System.out.println(bucketName + (found ? " found ✓" : " missing ✗"));

                // 3. Test IAM permissions explicitly
                GetBucketAclRequest aclRequest = GetBucketAclRequest.builder()
                        .bucket(bucketName)
                        .build();
                s3.getBucketAcl(aclRequest);  // Will fail if permissions issue
                System.out.println("Bucket permissions validated ✓");

                // 4. Verify region matches bucket location
                GetBucketLocationResponse bucketLocation = s3.getBucketLocation(
                        GetBucketLocationRequest.builder().bucket(bucketName).build()
                );
                Region bucketRegion = Region.of(bucketLocation.locationConstraintAsString());
                System.out.println("Bucket resides in: " + bucketRegion);

                if (!awsRegion.equals(bucketRegion)) {
                    System.err.println("⚠️ Client region " + awsRegion +
                            " doesn't match bucket region " + bucketRegion);
                }
            } catch (S3Exception e) {
                System.err.println("❗ S3 Validation Failure: " + e.awsErrorDetails().errorMessage());
                if (e.statusCode() == 403) {
                    System.err.println("Detected 403 - Possible causes:");
                    System.err.println("a. Bucket owner ≠ credentials account");
                    System.err.println("b. Explicit deny in bucket ACL");
                    System.err.println("c. AWS service IAM delay (try adding sleep)");
                }
            }

            Map<String, String> properties = Map.of(
                    "warehouse", bucketName, // Replace with your S3 path
                    "catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog",
                    "io-impl", "org.apache.iceberg.aws.s3.S3FileIO",
                    "s3.region", args[0]
            );
            catalog.initialize(catalogName, properties);

            // Get the table
            TableIdentifier tableId = TableIdentifier.of(Namespace.of(databaseName), tableName);
            Table table = catalog.loadTable(tableId);

            System.out.println("Loaded table: " + tableId);

            // Step 1: Scan table for files
            Map<String, Integer> uriCountMap = new HashMap<>();
            try (CloseableIterable<FileScanTask> files = table.newScan().planFiles()) {
                for (FileScanTask filetask : files) {
                    String uri = filetask.file().location();
                    uriCountMap.put(uri, uriCountMap.getOrDefault(uri, 0) + 1);
                }
            }

            // Step 2: Identify invalid URIs
            List<String> urisToDelete = new ArrayList<>();
            for (Map.Entry<String, Integer> entry : uriCountMap.entrySet()) {
                String uri = entry.getKey();
                if (entry.getValue() > 1) {
                    urisToDelete.add(uri); // Duplicate
                    System.out.println("Found duplicate URI: " + uri);
                } else if (!s3ObjectExists(s3, uri)) {
                    urisToDelete.add(uri); // Missing in S3
                    System.out.println("Found missing URI: " + uri);
                }
            }

            System.out.println("Press \"ENTER\" to continue...");
            Scanner scanner = new Scanner(System.in);
            scanner.nextLine();

            // Step 3: Batch delete invalid URIs
            Tasks.foreach(urisToDelete)
                    .retry(3)
                    .exponentialBackoff(100, 5000, 600000, 2.0)
                    .throwFailureWhenFinished()
                    .run(uri -> {
                        System.out.println("Attempting to remove file reference: " + uri);
                        table.io().deleteFile(uri);
                        table.newDelete().deleteFile(uri).commit();
                        System.out.println("Successfully deleted file reference: " + uri);
                    });

            System.out.println("Operation completed successfully");

        } catch (Exception e) {
            System.err.println("Error removing file reference: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}