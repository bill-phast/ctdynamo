package ai.phast.ctdynamo;

public final class Key<PartitionT, SortT> {

    private final PartitionT partition;

    private final SortT sort;

    public Key(PartitionT partition, SortT sort) {
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
