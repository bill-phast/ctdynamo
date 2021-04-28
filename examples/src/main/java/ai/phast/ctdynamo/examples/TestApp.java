package ai.phast.ctdynamo.examples;

import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import java.time.Instant;

public class TestApp {

    public static void main(String[] args) {
        var http = ApacheHttpClient.create();
        var baseDynamo = DynamoDbClient.builder()
            .httpClient(http)
            .region(Region.US_EAST_2)
            .build();
        var batchesTable = new LogBatchDynamoTable(baseDynamo, "bill-dcsm-logBatches");
        var value = batchesTable.get("LOGGER_TEST:bill:178b33ea7c8-1", Instant.parse("2021-04-08T20:48:22.513Z"));
        System.out.println("Value = " + value);
        baseDynamo.close();
        http.close();
    }
}
