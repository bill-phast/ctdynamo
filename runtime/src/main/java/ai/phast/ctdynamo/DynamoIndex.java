package ai.phast.ctdynamo;

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

public abstract class DynamoIndex<T, PartitionT, SortT> extends DynamoCodec<T> {

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

    public Query query(PartitionT partitionValue) {
        return new Query(partitionValue);
    }

    private QueryResponse invoke(Query query) {
        var request = buildQueryRequest(query);
        return (client == null ? asyncClient.query(request).join() : client.query(request));
    }

    private CompletableFuture<QueryResponse> invokeAsync(Query query) {
        var request = buildQueryRequest(query);
        return (asyncClient == null ? CompletableFuture.supplyAsync(() -> client.query(request)) : asyncClient.query(request));
    }

    private QueryRequest buildQueryRequest(Query query) {
        var dynamoQueryBuilder = QueryRequest.builder().tableName(tableName);
        if (indexName != null) {
            dynamoQueryBuilder.indexName(indexName);
        }
        var attributeValues = new HashMap<String, AttributeValue>();
        if (query.pageSize < Integer.MAX_VALUE) {
            dynamoQueryBuilder.limit(query.pageSize);
        } else if (query.limit < Integer.MAX_VALUE) {
            dynamoQueryBuilder.limit(query.limit);
        }
        dynamoQueryBuilder.keyConditionExpression(query.keyExpression == null ? "#p = :p" : query.keyExpression);
        attributeValues.put(":p", partitionValueToAttributeValue(query.partitionValue));
        if (query.sort1 != null) {
            attributeValues.put(":s1", sortValueToAttributeValue(query.sort1));
        }
        if (query.sort2 != null) {
            attributeValues.put(":s2", sortValueToAttributeValue(query.sort2));
        }
        if (query.startKey != null) {
            dynamoQueryBuilder.exclusiveStartKey(query.startKey);
        }
        return dynamoQueryBuilder
                   .expressionAttributeNames(Map.of("#p", partitionKeyAttribute, "#s", sortKeyAttribute))
                   .expressionAttributeValues(attributeValues)
                   .scanIndexForward(query.scanForward)
                   .build();
    }

    protected abstract AttributeValue partitionValueToAttributeValue(PartitionT partitionValue);

    protected abstract AttributeValue sortValueToAttributeValue(SortT sortValue);

    @Override
    public final AttributeValue encode(T value) {
        return AttributeValue.builder().m(encodeToMap(value)).build();
    }

    public abstract Map<String, AttributeValue> encodeToMap(T value);

    @Override
    public final T decode(AttributeValue dynamoValue) {
        return decode(dynamoValue.m());
    }

    public abstract T decode(Map<String, AttributeValue> map);

    public abstract Map<String, AttributeValue> getExclusiveStart(T value);

    public final class Query {

        private final PartitionT partitionValue;

        /** Our key expression. Null if we are only checking the partition key */
        private String keyExpression;

        private SortT sort1;

        private SortT sort2;

        private boolean scanForward = true;

        private Map<String, AttributeValue> startKey;

        private int limit = Integer.MAX_VALUE;

        private int pageSize = Integer.MAX_VALUE;

        private Query(PartitionT partitionValue) {
            if (partitionValue == null) {
                throw new NullPointerException("partitionValue must not be null");
            }
            this.partitionValue = partitionValue;
        }

        public Query sortBetween(SortT lo, SortT hi) {
            if (keyExpression != null) {
                throw new IllegalArgumentException("Only one sort expression can be used");
            }
            keyExpression = "#p = :p AND #s BETWEEN :s1 AND :s2";
            sort1 = lo;
            sort2 = hi;
            return this;
        }

        public Query sortAbove(SortT bound, boolean inclusive) {
            if (keyExpression != null) {
                throw new IllegalArgumentException("Only one sort expression can be used");
            }
            sort1 = bound;
            keyExpression = (inclusive ? "#p = :p AND #s >= :s1" : "#p = :p AND #s > :s1");
            return this;
        }

        public Query sortBelow(SortT bound, boolean inclusive) {
            if (keyExpression != null) {
                throw new IllegalArgumentException("Only one sort expression can be used");
            }
            sort1 = bound;
            keyExpression = (inclusive ? "#p = :p AND #s <= :s1" : "#p = :p AND #s < :s1");
            return this;
        }

        public Query sortPrefix(SortT prefix) {
            if (keyExpression != null) {
                throw new IllegalArgumentException("Only one sort expression can be used");
            }
            sort1 = prefix;
            keyExpression = "#p = :p AND begins_with(#s, :s1)";
            return this;
        }

        public Query scanForward(boolean value) {
            scanForward = value;
            return this;
        }

        public Query limit(int value) {
            limit = value;
            return this;
        }

        public Query pageSize(int value) {
            pageSize = value;
            return this;
        }

        public Query startKey(Map<String, AttributeValue> value) {
            startKey = value;
            return this;
        }

        public QueryResponse invoke() {
            return DynamoIndex.this.invoke(this);
        }

        public CompletableFuture<QueryResponse> invokeAsync() {
            return DynamoIndex.this.invokeAsync(this);
        }
    }
}
