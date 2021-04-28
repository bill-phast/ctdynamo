package ai.phast.ctdynamo.examples;

import ai.phast.ctdynamo.annotations.DynamoAttribute;
import ai.phast.ctdynamo.annotations.DynamoItem;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.time.Instant;

import lombok.Setter;
import lombok.ToString;

/**
 * This class represents one log entry in our log database.
 */
@Setter
@Getter
@ToString
@EqualsAndHashCode
@DynamoItem(DynamoItem.Output.CODEC)
public class LogEntry {

    /** The date when the message was produced. */
    @DynamoAttribute(codec = InstantCodec.class)
    private Instant date;

    /** The priority of message. */
    @DynamoAttribute
    private Priority priority;

    /** The message. */
    @DynamoAttribute
    private String message;

}
