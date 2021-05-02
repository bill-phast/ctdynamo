package ai.phast.ctdynamo;

import java.util.ArrayList;
import java.util.List;

/**
 * The output of any "extended" get, put, or delete command. This returns the item returned by the non-extended version
 * and the consumed capacity of the operation.
 * @param <T> The item returned
 */
public class ExtendedBatchResult<T, UnprocessedT> {

    private final List<T> items = new ArrayList<>();

    private final List<UnprocessedT> unprocessedValues = new ArrayList<>();

    private final CapacityUsed capacity = new CapacityUsed();

    public ExtendedBatchResult() {
    }

    public List<T> getItems() {
        return items;
    }

    public List<UnprocessedT> getUnprocessedValues() {
        return unprocessedValues;
    }

    public CapacityUsed getCapacity() {
        return capacity;
    }

}
