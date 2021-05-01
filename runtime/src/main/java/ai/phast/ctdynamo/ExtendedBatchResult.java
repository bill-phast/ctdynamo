package ai.phast.ctdynamo;

import software.amazon.awssdk.services.dynamodb.model.ConsumedCapacity;

import java.util.ArrayList;
import java.util.List;

/**
 * The output of any "extended" get, put, or delete command. This returns the item returned by the non-extended version
 * and the consumed capacity of the operation.
 * @param <T> The item returned
 */
public class ExtendedBatchResult<T, PartitionT, SortT> {

    private final List<T> items = new ArrayList<>();

    private final List<Key<PartitionT, SortT>> unprocessedKeys = new ArrayList<>();

    private final CapacityUsed capacity = new CapacityUsed();

    public ExtendedBatchResult() {
    }

    public List<T> getItems() {
        return items;
    }

    public List<Key<PartitionT, SortT>> getUnprocessedKeys() {
        return unprocessedKeys;
    }

    public CapacityUsed getCapacity() {
        return capacity;
    }

    public static class Key<PartitionT, SortT> {

        private final PartitionT partition;

        private final SortT sort;

        Key(PartitionT partition, SortT sort) {
            this.partition = partition;
            this.sort = sort;
        }

        public PartitionT getPartition() {
            return partition;
        }

        public SortT getSort() {
            return sort;
        }
    }
}
