package ai.phast.ctdynamo;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.Map;

public abstract class DynamoTable<T, PartitionT, SortT> extends DynamoIndex<T> {

    public final T get(T value) {
        return get(getPartitionKey(value), getSortKey(value));
    }

    public abstract PartitionT getPartitionKey(T value);

    public abstract SortT getSortKey(T value);

    public T get(PartitionT partitionValue, SortT sortValue) {
        throw new UnsupportedOperationException();
    }

    public void put(T value) {
        throw new UnsupportedOperationException();
    }

    public final void delete(T value) {
        delete(getPartitionKey(value), getSortKey(value));
    }

    public void delete(PartitionT partitionKey, SortT sortKey) {
        throw new UnsupportedOperationException();
    }
}
