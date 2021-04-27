package ai.phast.ctdynamo;

import org.w3c.dom.Attr;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

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

    public QueryBuilder query(PartitionT partitionValue) {
        return new QueryBuilder(partitionValue);
    }

    private QueryResponse go(QueryBuilder query) {
        var dynamoQueryBuilder = QueryRequest.builder()
                                     .tableName(tableName);
        if (indexName != null) {
            dynamoQueryBuilder.indexName(indexName);
        }
        var attributeValues = new HashMap<String, AttributeValue>();
        var keyExpression = new StringBuilder("#p = :p");
        dynamoQueryBuilder.expressionAttributeNames(Map.of(
            "#p", partitionKeyAttribute,
            "#s", sortKeyAttribute))
            .expressionAttributeValues(attributeValues)
            .keyConditionExpression(keyExpression.toString())
            .scanIndexForward(query.scanForward);
        if (query.pageSize < Integer.MAX_VALUE) {
            dynamoQueryBuilder.limit(query.pageSize);
        } else if (query.limit < Integer.MAX_VALUE) {
            dynamoQueryBuilder.limit(query.limit);
        }
        attributeValues.put(":p", partitionValueToAttributeValue(query.partitionValue));
        if (query.sortLo != null) {
            attributeValues.put(":s1", sortValueToAttributeValue(query.sortLo));
            keyExpression.append(query.loInclusive ? " AND #s >= :s1" : " AND #s > :s1");
        }
        if (query.sortHi != null) {
            attributeValues.put(":s2", sortValueToAttributeValue(query.sortHi));
            keyExpression.append(query.hiInclusive ? " AND #s <= :s2" : " AND #s < :s2");
        }
        if (query.startKey != null) {
            dynamoQueryBuilder.exclusiveStartKey(query.startKey);
        }
        return client.query(dynamoQueryBuilder.build());
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

    public final class QueryBuilder {

        private final PartitionT partitionValue;

        private SortT sortLo;

        private boolean loInclusive = true;

        private SortT sortHi;

        private boolean hiInclusive = true;

        private boolean scanForward = true;

        private Map<String, AttributeValue> startKey;

        private int limit = Integer.MAX_VALUE;

        private int pageSize = Integer.MAX_VALUE;

        private QueryBuilder(PartitionT partitionValue) {
            this.partitionValue = partitionValue;
        }

        public QueryBuilder sortBounds(SortT lo, boolean loInclusive, SortT hi, boolean hiInclusive) {
            sortLo = lo;
            this.loInclusive = loInclusive;
            sortHi = hi;
            this.hiInclusive = hiInclusive;
            return this;
        }

        public QueryBuilder scanForward(boolean value) {
            scanForward = value;
            return this;
        }

        public QueryBuilder limit(int value) {
            limit = value;
            return this;
        }

        public QueryBuilder pageSize(int value) {
            pageSize = value;
            return this;
        }

        public QueryBuilder startKey(Map<String, AttributeValue> value) {
            startKey = value;
            return this;
        }

        public QueryResponse go() {
            return DynamoIndex.this.go(this);
        }
    }
}
