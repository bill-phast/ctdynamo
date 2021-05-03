package ai.phast.ctdynamo;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;

import java.util.Map;

public final class Scan<T> {

    private final ScanRequest.Builder builder = ScanRequest.builder();

    private final DynamoIndex<T, ?, ?> index;

    private int limit = -1;

    private int pageSize = -1;

    Scan(DynamoIndex<T, ?, ?> index, int segment, int numSegments) {
        this.index = index;
        builder.tableName(index.getTableName()).indexName(index.getIndexName());
        var indexName = index.getIndexName();
        if (indexName != null) {
            builder.indexName(indexName);
        }
        if (numSegments > 1) {
            builder.segment(segment).totalSegments(numSegments);
        }
    }

    public Scan<T> limit(int value) {
        limit = value;
        return this;
    }

    public Scan<T> pageSize(int value) {
        pageSize = value;
        return this;
    }

    public Scan<T> startKey(Map<String, AttributeValue> value) {
        builder.exclusiveStartKey(value);
        return this;
    }

    public IterableResult<T, ?> invoke() {
        if (pageSize <= 0) {
            if (limit >= 0) {
                builder.limit(limit);
            }
        } else {
            builder.limit(pageSize);
        }
        return new ScanResult<>(index, builder, limit);
    }
}
