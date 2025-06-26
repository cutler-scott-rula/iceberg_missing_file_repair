package org.rula;

import org.apache.iceberg.Table;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.util.Tasks;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.Map;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class IcebergMetadataRepair {

    private static AwsCredentialsProvider resolveCredentials(String profileName) {
        return ProfileCredentialsProvider.builder()
                .profileName(profileName)
                .build();
    }

    public static void main(String[] args) {
        if (args.length < 4) {
            System.err.println("Usage: IcebergMetadataRepair <catalogName> <databaseName> <tableName> <filePath>");
            System.exit(1);
        }

        String catalogName = args[0];
        String databaseName = args[1];
        String tableName = args[2];
        String filePath = args[3]; // Path to the URI list file


        try {
            // Create AWS clients with profile credentials
            S3Client s3 = S3Client.builder()
                    .credentialsProvider(ProfileCredentialsProvider.create())
                    .build();

            GlueClient glue = GlueClient.builder()
                    .credentialsProvider(ProfileCredentialsProvider.create())
                    .build();

            // Configure Iceberg catalog
            GlueCatalog catalog = new GlueCatalog();
            Map<String, String> properties = Map.of(
                    "warehouse", "s3://rula-securitylake-prod20250506195612306300000005", // Replace with your S3 path
                    "catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog",
                    "io-impl", "org.apache.iceberg.aws.s3.S3FileIO"
            );
            catalog.initialize(catalogName, properties);

            // Get the table
            TableIdentifier tableId = TableIdentifier.of(Namespace.of(databaseName), tableName);
            Table table = catalog.loadTable(tableId);

            System.out.println("Loaded table: " + tableId);

            List<String> uris = Files.readAllLines(Paths.get(filePath));
            System.out.println("Loaded " + uris.size() + " URIs to delete.");

            // Remove the file reference from metadata
            Tasks.foreach(uris)
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