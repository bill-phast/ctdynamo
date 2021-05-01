package ai.phast.ctdynamo;

import software.amazon.awssdk.services.dynamodb.model.Capacity;
import software.amazon.awssdk.services.dynamodb.model.ConsumedCapacity;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class CapacityUsed {

    private static final Map<String, ReadWrite> EMPTY_INDEXES = Collections.emptyMap();

    private double totalRead;
    private double totalWrite;

    private double tableRead;
    private double tableWrite;

    private Map<String, ReadWrite> indexes;

    /**
     * Default constructor. Gives us all nulls.
     */
    public CapacityUsed() {
    }

    /**
     * Constructor based on a raw capacity.
     * @param raw The capacity from Dynamo.
     */
    CapacityUsed(ConsumedCapacity raw) {
        add(raw);
    }

    public double getTotalRead() {
        return totalRead;
    }

    public double getTotalWrite() {
        return totalWrite;
    }

    public double getTableRead() {
        return tableRead;
    }

    public double getTableWrite() {
        return tableWrite;
    }

    public Map<String, ReadWrite> getIndexes() {
        return indexes == null ? EMPTY_INDEXES : indexes;
    }

    void add(ConsumedCapacity raw) {
        if (raw == null) {
            return;
        }
        totalRead += zeroNull(raw.readCapacityUnits());
        totalWrite += zeroNull(raw.writeCapacityUnits());
        var table = raw.table();
        if (table != null) {
            tableRead += zeroNull(table.readCapacityUnits());
            tableWrite += zeroNull(table.writeCapacityUnits());
        }
        if (raw.hasGlobalSecondaryIndexes()) {
            indexes = updateIndexes(indexes, raw.globalSecondaryIndexes());
        }
        if (raw.hasLocalSecondaryIndexes()) {
            indexes = updateIndexes(indexes, raw.localSecondaryIndexes());
        }
    }

    private static Map<String, ReadWrite> updateIndexes(Map<String, ReadWrite> indexes, Map<String, Capacity> rawIndexes) {
        if (indexes == null) {
            indexes = new HashMap<>();
        }
        for (var indexName: rawIndexes.keySet()) {
            indexes.computeIfAbsent(indexName, key -> new ReadWrite()).add(rawIndexes.get(indexName));
        }
        return indexes;
    }

    private static double zeroNull(Double x) {
        return x == null ? 0.0 : x;
    }

    public static class ReadWrite {

        private double read;
        private double write;

        void add(Capacity capacity) {
            read += zeroNull(capacity.readCapacityUnits());
            write += zeroNull(capacity.writeCapacityUnits());
        }

        public double getRead() {
            return read;
        }

        public double getWrite() {
            return write;
        }
    }
}
