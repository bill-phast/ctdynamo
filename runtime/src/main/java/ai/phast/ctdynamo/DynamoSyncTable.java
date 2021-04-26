package ai.phast.ctdynamo;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

import java.util.Map;

/**
 * A class that fills in the DynamoTable functions with calls using a synchronous dynamo client. All concrete synchronous table
 * classese extend this.
 * @param <T>
 * @param <PartitionT>
 * @param <SortT>
 */
public abstract class DynamoSyncTable<T, PartitionT, SortT> extends DynamoTable<T, PartitionT, SortT> {

    private final DynamoDbClient client;

    final String tableName;

    public DynamoSyncTable(DynamoDbClient client, String tableName) {
        this.client = client;
        this.tableName = tableName;
    }

    @Override
    public T get(PartitionT partitionValue, SortT sortValue) {
        var result = client.getItem(GetItemRequest.builder()
                                        .tableName(tableName)
                                        .key(keysToMap(partitionValue, sortValue))
                                        .build());
        return decode(result.item());
    }

    @Override
    public void put(T value) {
        client.putItem(PutItemRequest.builder()
                           .tableName(tableName)
                           .item(encode(value))
                           .build());
    }

    @Override
    public void delete(PartitionT partitionKey, SortT sortKey) {
        client.deleteItem(DeleteItemRequest.builder()
                              .tableName(tableName)
                              .key(keysToMap(partitionKey, sortKey))
                              .build());
    }
}
