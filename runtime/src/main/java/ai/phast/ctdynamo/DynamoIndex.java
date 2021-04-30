package ai.phast.ctdynamo;

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.Map;
import java.util.Objects;

public abstract class DynamoIndex<T, PartitionT, SortT> {

    private final String tableName;

    private final String indexName;

    private final String partitionKeyAttribute;

    private final String sortKeyAttribute;

    private final DynamoDbClient client;

    private final DynamoDbAsyncClient asyncClient;

    public DynamoIndex(DynamoDbClient client, DynamoDbAsyncClient asyncClient,
                       String tableName, String indexName, String partitionKeyAttribute, String sortKeyAttribute) {
        if (client == null && asyncClient == null) {
            throw new NullPointerException("At least one of client or asyncClient must be non-null");
        }
        this.client = client;
        this.asyncClient = asyncClient;
        this.tableName = Objects.requireNonNull(tableName, "tableName must not be null");
        this.indexName = indexName;
        this.partitionKeyAttribute = Objects.requireNonNull(partitionKeyAttribute, "partitionKeyAttribute must not be null");
        this.sortKeyAttribute = sortKeyAttribute;
    }

    public final String getTableName() {
        return tableName;
    }

    /**
     * Get the name of this index. This will be null if this is a table.
     * @return The name of this index, or null if we are a table
     */
    public final String getIndexName() {
        return indexName;
    }

    protected final DynamoDbClient getClient() {
        return client;
    }

    protected final DynamoDbAsyncClient getAsyncClient() {
        return asyncClient;
    }

    protected final String getPartitionKeyAttribute() {
        return partitionKeyAttribute;
    }

    protected final String getSortKeyAttribute() {
        return sortKeyAttribute;
    }

    public Query<T, PartitionT, SortT> query(PartitionT partitionValue) {
        return new Query<>(this, partitionValue);
    }

    protected abstract AttributeValue partitionValueToAttributeValue(PartitionT partitionValue);

    protected abstract AttributeValue sortValueToAttributeValue(SortT sortValue);

    public abstract T decode(Map<String, AttributeValue> map);

    public abstract Map<String, AttributeValue> getExclusiveStart(T value);

}
