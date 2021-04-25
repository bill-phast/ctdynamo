package ai.phast.ctdynamo.examples;

import ai.phast.ctdynamo.annotations.DynamoDoc;
import ai.phast.ctdynamo.annotations.DynamoPartitionKey;

@DynamoDoc
public class LogBatch {

    private String id;

    @DynamoPartitionKey
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
}
