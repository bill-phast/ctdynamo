package ai.phast.ctdynamo;

import software.amazon.awssdk.services.dynamodb.model.ConsumedCapacity;

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
