package ai.phast.ctdynamo;

import software.amazon.awssdk.services.dynamodb.model.ConsumedCapacity;

/**
 * The output of any "extended" get, put, or delete command. This returns the item returned by the non-extended version
 * and the consumed capacity of the operation.
 * @param <T> The item returned
 */
public class ExtendedItemResult<T> {

    private final T item;

    private final ConsumedCapacity consumedCapacity;

    public ExtendedItemResult(T item, ConsumedCapacity consumedCapacity) {
        this.item = item;
        this.consumedCapacity = consumedCapacity;
    }

    public T getItem() {
        return item;
    }

    public ConsumedCapacity getConsumedCapacity() {
        return consumedCapacity;
    }
}
