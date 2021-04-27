package ai.phast.ctdynamo.examples;

import ai.phast.ctdynamo.annotations.DynamoAttribute;
import ai.phast.ctdynamo.annotations.DynamoItem;

import java.time.Instant;

/**
 * This class represents one log entry in our log database.
 */
@DynamoItem(DynamoItem.Output.CODEC)
public class LogEntry {

    /** The date when the message was produced. */
    private Instant date;

    /** The priority of message. */
    private String priority;

    /** The message. */
    private String message;

    @DynamoAttribute(codec=InstantCodec.class)
    public Instant getDate() {
        return date;
    }

    public void setDate(Instant value) {
        date = value;
    }

    public String getPriority() {
        return priority;
    }

    public void setPriority(String value) {
        priority = value;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String value) {
        message = value;
    }
}
