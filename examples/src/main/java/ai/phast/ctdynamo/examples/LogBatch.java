package ai.phast.ctdynamo.examples;

import ai.phast.ctdynamo.annotations.DynamoDoc;
import ai.phast.ctdynamo.annotations.DynamoPartitionKey;

@DynamoDoc
public class LogBatch {

    private String id;

    private int batchNum;

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
}
