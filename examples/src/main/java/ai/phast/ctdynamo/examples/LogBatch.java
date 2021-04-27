package ai.phast.ctdynamo.examples;

import ai.phast.ctdynamo.annotations.DynamoAttribute;
import ai.phast.ctdynamo.annotations.DynamoIgnore;
import ai.phast.ctdynamo.annotations.DynamoItem;
import ai.phast.ctdynamo.annotations.DynamoPartitionKey;
import ai.phast.ctdynamo.annotations.DynamoSortKey;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * This class represents one log batch, so that we could check all logs of current case.
 */
@DynamoItem
public class LogBatch {

    /** The service and instance attribute. */
    public static final String SERVICE_AND_INSTANCE_ATTRIBUTE = "serviceAndInstance";

    /** The date of first message attribute. */
    public static final String DATE_OF_FIRST_MESSAGE_ATTRIBUTE = "dateOfFirstMessage";

    /** The case id attribute. */
    public static final String CASE_ID_ATTRIBUTE = "caseId";

    /** The error attribute. */
    public static final String ERROR_ATTRIBUTE = "error";

    /** The case id GSI */
    public static final String CASE_ID_INDEX = "caseId";

    /** The error index as secondary partition key. */
    public static final String ERROR_INDEX = "error";

    /** Used to extract the service, name, and instance from our partition key */
    private static final Pattern PARTITION_KEY_PARSER = Pattern.compile("(?<service>[^:]+):(?<stage>[^:]+):(?<instance>.+)");

    /** The service that current instance belongs to. */
    private String service;

    /** The stage of the service that generated these logs */
    private String stage;

    /** The lambda instance that currently running. */
    private String instance;

    /** The case id. */
    private String caseId;

    /** The list of log entries. */
    private List<LogEntry> logEntries;

    /** The time, in seconds since the epoch, when this database row should be deleted. */
    private long ttl;

    /** The error, which represent if there is an error level entry in current batch. */
    private String error;

    /**
     * Service, stage, and instance are the primary partition key.
     * They saved together in dynamo to work as a partition key, but separate in Java.
     * Format is like "WEBAPP:gamma:123456".
     * @return the service and instance combo as a string.
     * @throws NullPointerException If any of service, stage, or instance are null
     */
    @DynamoPartitionKey
    @JsonIgnore
    public String getServiceAndInstance() {
        return service.toString() + ':' + Objects.requireNonNull(stage) + ':' + Objects.requireNonNull(instance);
    }

    /**
     * Split the serviceAndInstance to service, stage, and instance.
     * @param serviceAndInstance A string of service, stage, and instance.
     */
    public void setServiceAndInstance(String serviceAndInstance) {
        var matcher = PARTITION_KEY_PARSER.matcher(serviceAndInstance);
        if (!matcher.matches()) {
            throw new RuntimeException("Cannot parse serviceAndInstance: " + serviceAndInstance);
        }
        try {
            service = matcher.group("service");
        } catch (Exception ex) {
            throw new RuntimeException("Unknown service: " + serviceAndInstance);
        }
        stage = matcher.group("stage");
        instance = matcher.group("instance");
    }

    /**
     * Get the service.
     * We have service as a variable in Java, but we store it with instance together as a string in DynamoDB,
     * so we need this get function to add the DynamoDbIgnore.
     * @return the service as a Service enum.
     */
    @DynamoIgnore
    public String getService() {
        return service;
    }

    /**
     * Get the stage.
     * We have stage as a variable in Java, but we store it with service together as a string in DynamoDB,
     * so we need this get function to add the DynamoDbIgnore.
     * @return the instance as a string.
     */
    @DynamoIgnore
    public String getStage() {
        return stage;
    }

    /**
     * Get the instance.
     * We have instance as a variable in Java, but we store it with service together as a string in DynamoDB,
     * so we need this get function to add the DynamoDbIgnore.
     * @return the instance as a string.
     */
    @DynamoIgnore
    public String getInstance() {
        return instance;
    }

    public void setInstance(String value) {
        instance = value;
    }

    /**
     * Case ID is the secondary partition key.
     * Get the case ID.
     * @return case id as a string.
     */
    public String getCaseId() {
        return caseId;
    }

    public void setCaseId(String caseId) {
        this.caseId = caseId;
    }

    /**
     * Get the first date of log from log entries.
     * @return the date of first message as a time instant.
     */
    @DynamoSortKey(codec = InstantCodec.class)
    public Instant getDateOfFirstMessage() {
        if (logEntries == null) {
            return null;
        } else {
            return logEntries.stream()
                       .map(LogEntry::getDate)
                       .min(Comparator.naturalOrder())
                       .orElse(null);
        }
    }

    public void setDateOfFirstMessage(Instant value) {
    }

    /**
     * Get the log with priority of "ERROR".
     * @return the value of error. It's non-null when any entry from batch has priority ERROR.
     */
    public String getError() {
        if (logEntries == null) {
            return null;
        } else {
            return logEntries.stream().anyMatch(entry -> entry.getPriority().equals("ERROR")) ? "ERROR" : null;
        }
    }

    public void setError(String value) {
    }

    public List<LogEntry> getLogEntries() {
        return logEntries;
    }

    public void setLogEntries(List<LogEntry> value) {
        logEntries = value;
    }
}
