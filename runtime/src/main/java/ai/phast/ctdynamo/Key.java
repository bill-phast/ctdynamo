package ai.phast.ctdynamo;

import java.util.Objects;

/**
 * Class that represents a key of a Dynamo table. This is primarily useful in batch operation but may also be used
 * in singleton get or delete requests.
 * @param <PartitionT> The type of the partition key
 * @param <SortT> The type of the stort key, or Void if there is no sort key
 */
public final class Key<PartitionT, SortT> {

    /** The partition key */
    private final PartitionT partition;

    /** The sort key, or null if there is no sort key */
    private final SortT sort;

    /**
     * Build a key. The partition value may not be null. The sort value should only be null if SortT is Void, but that cannot
     * be enforced due to Java's erasure system
     * @param partition The partition value
     * @param sort The sort value
     * @throws NullPointerException If partition is null
     */
    public Key(PartitionT partition, SortT sort) {
        this.partition = Objects.requireNonNull(partition);
        this.sort = sort;
    }

    /**
     * Get the partition value
     * @return The partition value
     */
    public PartitionT getPartition() {
        return partition;
    }

    /**
     * Get the sort value
     * @return The sort value
     */
    public SortT getSort() {
        return sort;
    }

    @Override
    public int hashCode() {
        var value = partition.hashCode() * 31;
        if (sort != null) {
            value += sort.hashCode();
        }
        return value;
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if (other instanceof Key) {
            var peer = (Key<?, ?>)other;
            return partition.equals(peer.partition) && Objects.equals(sort, peer.sort);
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + partition + ", " + sort + "]";
    }
}
