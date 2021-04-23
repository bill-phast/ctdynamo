package ai.phast.ctdynamo;

public abstract class DynamoTable<T, PartitionT, SortT> extends DynamoIndex<T> {

    public final T get(T value) {
        return get(getPartitionKey(value), getSortKey(value));
    }

    public abstract T get(PartitionT partitionValue, SortT sortValue);

    public abstract void put(T value);

    public final void delete(T value) {
        delete(getPartitionKey(value), getSortKey(value));
    }

    public abstract void delete(PartitionT partitionKey, SortT sortKey);

    public abstract PartitionT getPartitionKey(T value);

    public abstract SortT getSortKey(T value);
}
