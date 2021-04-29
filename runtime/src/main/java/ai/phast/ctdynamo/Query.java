package ai.phast.ctdynamo;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public final class Query<T, PartitionT, SortT> {

    private final Map<String, AttributeValue> values = new HashMap<>();

    private final QueryRequest.Builder builder = QueryRequest.builder()
        .expressionAttributeValues(values);

    private final DynamoIndex<T, PartitionT, SortT> index;

    private boolean sortSet = false;

    private Integer limit;

    private Integer pageSize;

    Query(DynamoIndex<T, PartitionT, SortT> index, PartitionT partitionValue) {
        this.index = index;
        if (partitionValue == null) {
            throw new NullPointerException("partitionValue must not be null");
        }
        values.put(":p", index.partitionValueToAttributeValue(partitionValue));
    }

    public Query<T, PartitionT, SortT> sortBetween(SortT lo, SortT hi) {
        if (sortSet) {
            throw new IllegalArgumentException("Only one sort expression can be used");
        }
        sortSet = true;
        builder.keyConditionExpression("#p = :p AND #s BETWEEN :s1 AND :s2");
        values.put(":s1", index.sortValueToAttributeValue(lo));
        values.put(":s2", index.sortValueToAttributeValue(hi));
        return this;
    }

    public Query<T, PartitionT, SortT> sortAbove(SortT bound, boolean inclusive) {
        if (sortSet) {
            throw new IllegalArgumentException("Only one sort expression can be used");
        }
        sortSet = true;
        builder.keyConditionExpression(inclusive ? "#p = :p AND #s >= :s1" : "#p = :p AND #s > :s1");
        values.put(":s1", index.sortValueToAttributeValue(bound));
        return this;
    }

    public Query<T, PartitionT, SortT> sortBelow(SortT bound, boolean inclusive) {
        if (sortSet) {
            throw new IllegalArgumentException("Only one sort expression can be used");
        }
        sortSet = true;
        builder.keyConditionExpression(inclusive ? "#p = :p AND #s <= :s1" : "#p = :p AND #s < :s1");
        values.put(":s1", index.sortValueToAttributeValue(bound));
        return this;
    }

    public Query<T, PartitionT, SortT> sortPrefix(SortT prefix) {
        if (sortSet) {
            throw new IllegalArgumentException("Only one sort expression can be used");
        }
        sortSet = true;
        builder.keyConditionExpression("#p = :p AND begins_with(#s, :s1)");
        values.put(":s1", index.sortValueToAttributeValue(prefix));
        return this;
    }

    public Query<T, PartitionT, SortT> scanForward(boolean value) {
        builder.scanIndexForward(value);
        return this;
    }

    public Query<T, PartitionT, SortT> limit(int value) {
        limit = value;
        return this;
    }

    public Query<T, PartitionT, SortT> pageSize(int value) {
        pageSize = value;
        return this;
    }

    public Query<T, PartitionT, SortT> startKey(Map<String, AttributeValue> value) {
        builder.exclusiveStartKey(value);
        return this;
    }

    public QueryResult<T> invoke() {
        if (pageSize == null) {
            if (limit != null) {
                builder.limit(limit);
            }
        } else {
            builder.limit(pageSize);
        }
        return new QueryResult<>(index, builder, limit);
    }
}
