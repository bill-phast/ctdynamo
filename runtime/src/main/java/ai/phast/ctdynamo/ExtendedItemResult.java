package ai.phast.ctdynamo;

import software.amazon.awssdk.services.dynamodb.model.ConsumedCapacity;

/**
 * The output of any "extended" get, put, or delete command. This returns the item returned by the non-extended version
 * and the consumed capacity of the operation.
 * @param <T> The item returned
 */
public class ExtendedItemResult<T> {

    private final T item;

    private final CapacityUsed capacity;

    ExtendedItemResult(T item, ConsumedCapacity rawCapacity) {
        this(item, new CapacityUsed(rawCapacity));
    }

    public ExtendedItemResult(T item, CapacityUsed capacity) {
        this.item = item;
        this.capacity = capacity;
    }

    public T getItem() {
        return item;
    }

    public CapacityUsed getCapacity() {
        return capacity;
    }
}
