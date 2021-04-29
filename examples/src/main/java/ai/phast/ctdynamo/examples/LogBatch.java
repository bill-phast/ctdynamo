package ai.phast.ctdynamo.examples;

import ai.phast.ctdynamo.annotations.DynamoAttribute;
import ai.phast.ctdynamo.annotations.DynamoIgnore;
import ai.phast.ctdynamo.annotations.DynamoItem;
import ai.phast.ctdynamo.annotations.DynamoPartitionKey;
import ai.phast.ctdynamo.annotations.DynamoSortKey;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

import lombok.Setter;
import lombok.ToString;

/**
 * This class represents one log batch, so that we could check all logs of current case.
 */
@Setter
@Getter
@EqualsAndHashCode
@ToString
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

    /** The service that current instance belongs to. In dynamo persisted as part of the partition key, not as a separate attribute */
    @DynamoIgnore
    private Service service;

    /** The stage of the service that generated these logs. In dynamo persisted as part of the partition key, not as a separate attribute */
    @DynamoIgnore
    private String stage;

    /** The lambda instance that currently running. In dynamo persisted as part of the partition key, not as a separate attribute */
    @DynamoIgnore
    private String instance;

    @DynamoAttribute
    private Integer boxyInt;

    @DynamoAttribute
    private Boolean boxyBool;

    /** The case id. */
    @DynamoAttribute(CASE_ID_ATTRIBUTE)
    private String caseId;

    /** The list of log entries. */
    @DynamoAttribute
    private List<LogEntry> logEntries;

    /** The time, in seconds since the epoch, when this database row should be deleted. */
    @DynamoAttribute
    private long ttl;

    /**
     * Service, stage, and instance are the primary partition key.
     * They saved together in dynamo to work as a partition key, but separate in Java.
     * Format is like "WEBAPP:gamma:123456".
     * @return the service and instance combo as a string.
     * @throws NullPointerException If any of service, stage, or instance are null
     */
    @DynamoPartitionKey(SERVICE_AND_INSTANCE_ATTRIBUTE)
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
            service = Service.valueOf(matcher.group("service"));
        } catch (Exception ex) {
            throw new RuntimeException("Unknown service: " + serviceAndInstance);
        }
        stage = matcher.group("stage");
        instance = matcher.group("instance");
    }

    /**
     * Get the first date of log from log entries.
     * @return the date of first message as a time instant.
     */
    @DynamoSortKey(value=DATE_OF_FIRST_MESSAGE_ATTRIBUTE, codec=InstantCodec.class)
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
        // Do nothing
    }

    /**
     * Get the log with priority of "ERROR".
     * @return the value of error. It's non-null when any entry from batch has priority ERROR.
     */
    @DynamoAttribute(ERROR_ATTRIBUTE)
    @JsonIgnore
    public String getError() {
        if (logEntries == null) {
            return null;
        } else {
            return logEntries.stream().anyMatch(entry -> entry.getPriority() == Priority.ERROR) ? "ERROR" : null;
        }
    }

    public void setError(String value) {
        // Do nothing
    }
}
