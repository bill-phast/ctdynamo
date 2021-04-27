package ai.phast.ctdynamo.examples;

import ai.phast.ctdynamo.annotations.DynamoAttribute;
import ai.phast.ctdynamo.annotations.DynamoDoc;
import ai.phast.ctdynamo.annotations.DynamoPartitionKey;

import java.time.Instant;

@DynamoDoc()
public class LogBatch {

    private String id;

    private int batchNum;

    private Instant date;

    @DynamoPartitionKey
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public int getBatchNum() {
        return batchNum;
    }

    public void setBatchNum(int batchNum) {
        this.batchNum = batchNum;
    }

    @DynamoAttribute(codec = InstantCodec.class)
    public Instant getDate() {
        return date;
    }

    public void setDate(Instant value) {
        date = value;
    }
}
