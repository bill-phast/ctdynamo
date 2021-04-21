package ai.phast.dcs.logger;

import ai.phast.dcs.shared.DcsConstants;
import ai.phast.dcsm.shared.DcsmConstants;
import ai.phast.dcsm.shared.logDb.LogBatch;
import ai.phast.dcsm.shared.logDb.LogEntry;
import ai.phast.dcsm.shared.logDb.LogInstance;
import ai.phast.dcsm.shared.logDb.Priority;
import ai.phast.dcsm.shared.logDb.Service;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbIndex;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryConditional;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.CreateLogStreamRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.InputLogEvent;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsRequest;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Integration tests for the logger lambda. Writes to cloudwatch, then waits, and expects to see the messages appear
 * in dynamo within 30 seconds.
 */
public class Integration {

    public static final String LOG_GROUP_SUFFIX = "-dcs-loggerTest";

    private static CloudWatchLogsClient logsClient;

    private static DynamoDbTable<LogBatch> dcsBatches;

    private static DynamoDbIndex<LogBatch> dcsCaseIdIndex;

    private static DynamoDbIndex<LogBatch> dcsErrorIndex;

    private static DynamoDbTable<LogInstance> dcsInstances;

    private static DynamoDbTable<LogBatch> dcsmBatches;

    private static DynamoDbTable<LogInstance> dcsmInstances;

    private static final String INSTANCE = Long.toHexString(Instant.now().toEpochMilli());

    private static final AtomicInteger STREAM_COUNTER = new AtomicInteger();

    @BeforeAll
    public static void setup() {
        if (System.getenv("AWS_REGION") == null) {
            // Probably running in the IDE or on a desktop. In CodePipeline AWS_REGION will be set properly.
            System.setProperty("aws.region", "us-east-2");
        }

        // If you want to run this on your desktop or in the IDE, you will need to set the stage to match your name.
        // In CodePipeline, the stage will be set by an environment variable, which will override this value.
        System.setProperty("STAGE", "your name here");
        if (DcsmConstants.STAGE.equals("your name here")) {
            throw new IllegalStateException("When running on a desktop or in an IDE, you must put your stage name in the setProperty");
        }

        SdkHttpClient httpClient = ApacheHttpClient.create();
        logsClient = CloudWatchLogsClient.builder()
                         .httpClient(httpClient)
                         .region(DcsmConstants.REGION)
                         .build();
        var dynamoClient = DynamoDbEnhancedClient.builder()
                               .dynamoDbClient(DynamoDbClient.builder()
                                                   .httpClient(httpClient)
                                                   .region(DcsmConstants.REGION)
                                                   .build())
                               .build();
        var batchSchema = TableSchema.fromBean(LogBatch.class);
        var instanceSchema = TableSchema.fromBean(LogInstance.class);
        dcsBatches = dynamoClient.table(DcsConstants.LOG_BATCHES_TABLE, batchSchema);
        dcsCaseIdIndex = dcsBatches.index(LogBatch.CASE_ID_INDEX);
        dcsErrorIndex = dcsBatches.index(LogBatch.ERROR_INDEX);
        dcsInstances = dynamoClient.table(DcsConstants.LOG_INSTANCES_TABLE, instanceSchema);
        dcsmBatches = dynamoClient.table(DcsmConstants.LOG_BATCHES_TABLE, batchSchema);
        dcsmInstances = dynamoClient.table(DcsmConstants.LOG_INSTANCES_TABLE, instanceSchema);
    }

    @Test
    public void testLogger_shouldSanitizeInDcsm_whenPiiPresent() throws Exception {
        // Setup
        var streamId = openLogStream();
        var nowMs = Instant.now().minus(30, ChronoUnit.SECONDS).toEpochMilli();

        // Act
        var token = writeEvents(streamId, null,
            InputLogEvent.builder()
                .message("This is my message")
                .timestamp(nowMs)
                .build(),
            InputLogEvent.builder()
                .message("[WARN]: Second message with {pii=dangerous} information")
                .timestamp(nowMs + 1000)
                .build());
        writeEvents(streamId, token,
            InputLogEvent.builder()
                .timestamp(nowMs + 500)
                .message("Another message from another event")
                .build());

        // Verify
        retry(() -> {
            verifyEntries(dcsBatches, streamId,
                buildEntry(nowMs, Priority.INFO, "This is my message"),
                buildEntry(nowMs + 500, Priority.INFO, "Another message from another event"),
                buildEntry(nowMs + 1000, Priority.WARN, "Second message with {pii=dangerous} information"));
            verifyEntries(dcsmBatches, streamId,
                buildEntry(nowMs, Priority.INFO, "This is my message"),
                buildEntry(nowMs + 500, Priority.INFO, "Another message from another event"),
                buildEntry(nowMs + 1000, Priority.WARN, "Second message with {pii} information"));
        });
    }

    @Test
    public void testLogger_shouldMergeBatches_whenSameStartDate() throws Exception {
        // Setup
        var streamId = openLogStream();
        var nowMs = Instant.now().minus(30, ChronoUnit.SECONDS).toEpochMilli();

        // Act
        var token = writeEvents(streamId, null,
            InputLogEvent.builder()
                .message("[WARN][M-" + INSTANCE + "]: m1")
                .timestamp(nowMs)
                .build());
        writeEvents(streamId, token,
            InputLogEvent.builder()
                .message("[WARN][M-" + INSTANCE + "]: m2")
                .timestamp(nowMs)
                .build());

        // Verify
        retry(() -> verifyEntries(dcsBatches, dcsCaseIdIndex, streamId, "M-" + INSTANCE,
            buildEntry(nowMs, Priority.WARN, "m1"),
            buildEntry(nowMs, Priority.WARN, "m2")
        ));
    }

    @Test
    public void testLogger_shouldSetCaseId_whenCaseIdInMessage() throws Exception {
        // Setup
        var streamId = openLogStream();
        var nowMs = Instant.now().minus(30, ChronoUnit.SECONDS).toEpochMilli();

        // Act
        var token = writeEvents(streamId, null,
            InputLogEvent.builder()
                .message("[WARN]: No caseId")
                .timestamp(nowMs)
                .build(),
            InputLogEvent.builder()
                .message("[WARN][X-" + INSTANCE + "]: With caseId")
                .timestamp(nowMs + 1)
                .build());
        token = writeEvents(streamId, token,
            InputLogEvent.builder()
                .message("[INFO][X-" + INSTANCE + "]: caseId again")
                .timestamp(nowMs + 2)
                .build());
        writeEvents(streamId, token,
            InputLogEvent.builder()
                .message("[INFO]: No caseId again")
                .timestamp(nowMs + 3)
                .build());

        // Verify
        retry(() -> {
            verifyEntries(dcsBatches, streamId,
                buildEntry(nowMs, Priority.WARN, "No caseId"),
                buildEntry(nowMs + 3, Priority.INFO, "No caseId again"));
            verifyEntries(dcsBatches, dcsCaseIdIndex, streamId, "X-" + INSTANCE,
                buildEntry(nowMs + 1, Priority.WARN, "With caseId"),
                buildEntry(nowMs + 2, Priority.INFO, "caseId again"));
        });
    }

    @Test
    public void testLogger_shouldBeInErrorIndex_whenErrorsPresent() throws Exception {
        // Setup
        var streamId = openLogStream();
        var nowMs = Instant.now().minus(30, ChronoUnit.SECONDS).toEpochMilli();

        // Act
        var token = writeEvents(streamId, null,
            InputLogEvent.builder()
                .message("[INFO]: This is OK")
                .timestamp(nowMs)
                .build(),
            InputLogEvent.builder()
                .message("[ERROR]: Danger will robinson!")
                .timestamp(nowMs + 1)
                .build());
        writeEvents(streamId, token,
            InputLogEvent.builder()
                .message("[ERROR]: Error on its own")
                .timestamp(nowMs + 2)
                .build());

        // Verify
        retry(() -> {
            var errors = dcsErrorIndex.query(QueryConditional.sortGreaterThan(Key.builder()
                                                                                  .partitionValue("ERROR")
                                                                                  .sortValue(Instant.ofEpochMilli(nowMs - 1).toString())
                                                                                  .build()))
                             .stream()
                             .flatMap(page -> page.items().stream())
                             .flatMap(batch -> batch.getLogEntries().stream())
                             .collect(Collectors.toList());
            Assertions.assertTrue(errors.contains(
                buildEntry(nowMs + 1, Priority.ERROR, "Danger will robinson!")));
            Assertions.assertTrue(errors.contains(
                buildEntry(nowMs + 2, Priority.ERROR, "Error on its own")));
        });
    }

    @Test
    public void testLogger_shouldCreateInstance_onlyWhenPrefixFound() throws Exception {
        // Setup
        var instanceStreamId = openLogStream();
        var noInstanceStreamId = openLogStream();
        var now = Instant.now().truncatedTo(ChronoUnit.MILLIS).minus(30, ChronoUnit.SECONDS);
        var nowMs = now.toEpochMilli();

        // Act
        writeEvents(noInstanceStreamId, null,
            InputLogEvent.builder()
                .message("[INFO]: Blah blah blah")
                .timestamp(nowMs)
                .build());
        writeEvents(instanceStreamId, null,
            InputLogEvent.builder()
                .message("[INFO]: " + DcsmConstants.INSTANCE_STARTUP_PREFIX + " More text here")
                .timestamp(nowMs)
                .build());

        // Verify
        var expectedInstanceMessage = buildEntry(nowMs, Priority.INFO, DcsmConstants.INSTANCE_STARTUP_PREFIX + " More text here");
        var expectedNoInstanceMessage = buildEntry(nowMs, Priority.INFO, "Blah blah blah");
        var expectedInstance = new LogInstance();
        expectedInstance.setInstance(instanceStreamId);
        expectedInstance.setDateOfFirstMessage(now);
        expectedInstance.setService(Service.LOGGER_TEST);
        expectedInstance.setStage(DcsmConstants.STAGE);
        var keyInstance = new LogInstance();
        keyInstance.setService(Service.LOGGER_TEST);
        keyInstance.setStage(DcsmConstants.STAGE);
        keyInstance.setDateOfFirstMessage(now.minus(1, ChronoUnit.HOURS));
        keyInstance.setInstance("");
        retry(() -> {
            verifyEntries(dcsBatches, instanceStreamId, expectedInstanceMessage);
            verifyEntries(dcsmBatches, instanceStreamId, expectedInstanceMessage);
            verifyEntries(dcsBatches, noInstanceStreamId, expectedNoInstanceMessage);
            verifyEntries(dcsmBatches, noInstanceStreamId, expectedNoInstanceMessage);
            verifyInstances(dcsInstances, expectedInstance, keyInstance);
            verifyInstances(dcsmInstances, expectedInstance, keyInstance);
        });
    }

    /**
     * Verify that the proper instances are in the instance table.
     * @param table The table to check
     * @param expectedInstance The instance we expect to see in the table
     * @param keyInstance An instance with the proper service and stage and a date well before the start of this test run.
     *                    This is used to indicate where to start the search.
     */
    private void verifyInstances(DynamoDbTable<LogInstance> table, LogInstance expectedInstance, LogInstance keyInstance) {
        // Take all instances in the table, then filter the result to only include instances whose ID starts withours.
        // That will filter out instances from other test runs.
        var instances = table.query(QueryConditional.sortGreaterThan(table.keyFrom(keyInstance)))
                            .items().stream()
                            .filter(instance -> instance.getInstance().startsWith(INSTANCE))
                            .collect(Collectors.toList());

        // Make sure all instances have a sane TTL.
        for (var instance: instances) {
            Assertions.assertTrue(instance.getTtl() > instance.getDateOfFirstMessage().getEpochSecond(),
                "Instance " + instance + " has an invalid TTL");
            instance.setTtl(0); // Clear TTL so we can compare it against expected
        }
        Assertions.assertEquals(List.of(expectedInstance), instances);
    }

    /**
     * Open a new log stream with our test instance and a counter that gives this test a unique log stream
     * @return The name of the log stream
     */
    private String openLogStream() {
        var streamId = INSTANCE + "-" + STREAM_COUNTER.addAndGet(1);
        logsClient.createLogStream(CreateLogStreamRequest.builder()
                                       .logGroupName(DcsmConstants.STAGE + LOG_GROUP_SUFFIX)
                                       .logStreamName(streamId)
                                       .build());
        return streamId;
    }

    /**
     * Write events to a log stream
     * @param streamId The ID of the log stream
     * @param sequenceToken The sequence token, or null if this is the first event added to the stream
     * @param events The events to log
     * @return The next sequence token to use
     */
    private String writeEvents(String streamId, String sequenceToken, InputLogEvent... events) {
        var builder = PutLogEventsRequest.builder()
                          .logGroupName(DcsmConstants.STAGE + LOG_GROUP_SUFFIX)
                          .logStreamName(streamId)
                          .logEvents(events);
        if (sequenceToken != null) {
            builder.sequenceToken(sequenceToken);
        }
        return logsClient.putLogEvents(builder.build()).nextSequenceToken();
    }

    /**
     * Run a function every second, retrying on assertion failures, until it succeeds or 30 seconds elapse
     * @param func The function to call
     * @throws Exception On error
     */
    private void retry(Runnable func) throws Exception {
        for (var i = 0; i < 30; ++i) {
            try {
                func.run();
            } catch (AssertionFailedError e) {
                Thread.sleep(TimeUnit.SECONDS.toMillis(1));
                // Loop back, try again.
            }
        }
        func.run();  // One last try that throws out the errors.
    }

    /**
     * Verify that these entries are in the table under the given stream ID
     * @param table The table to search
     * @param streamId The stream ID to search in
     * @param expected The entries we expect to find
     */
    private void verifyEntries(DynamoDbTable<LogBatch> table, String streamId, LogEntry... expected) {
        verifyEntries(table, null, streamId, null, expected);
    }

    /**
     * Verify that these entries are in the table under the given stream ID and the given case ID
     * @param table The table to search
     * @param index The case ID index for the table
     * @param streamId The stream ID to search in
     * @param caseId The case ID to search under
     * @param expected The entries we expect to find
     */
    private void verifyEntries(DynamoDbTable<LogBatch> table, DynamoDbIndex<LogBatch> index, String streamId, String caseId, LogEntry... expected) {
        var expectedList = Arrays.asList(expected);
        Assertions.assertEquals(expectedList, getLogEntries(table, streamId, caseId));
        if (index != null) {
            Assertions.assertEquals(expectedList, getLogEntries(index, streamId, caseId));
        }
    }

    /**
     * Get log entries from a table
     * @param table The table to fetch from
     * @param streamId The stream ID to search under
     * @param caseId The case to filter by
     * @return A list of matching entries from the stream
     */
    private List<LogEntry> getLogEntries(DynamoDbTable<LogBatch> table, String streamId, String caseId) {
        var startBatch = new LogBatch();
        startBatch.setService(Service.LOGGER_TEST);
        startBatch.setStage(DcsmConstants.STAGE);
        startBatch.setInstance(streamId);
        startBatch.setCaseId(caseId);
        var batches = table.query(QueryConditional.sortGreaterThan(Key.builder()
                                                                      .partitionValue(startBatch.getServiceAndInstance())
                                                                      .sortValue(Instant.ofEpochMilli(0L).toString())
                                                                      .build()));
        return batches.items().stream()
                   .filter(batch -> Objects.equals(caseId, batch.getCaseId()))
                   .flatMap(batch -> batch.getLogEntries().stream())
                   .sorted(Comparator.comparing(LogEntry::getDate).thenComparing(LogEntry::getMessage))
                   .collect(Collectors.toList());
    }

    /**
     * Get log entries from an index
     * @param index The index to fetch from
     * @param streamId The stream ID to search under
     * @param caseId The case to filter by
     * @return A list of matching entries from the stream
     */
    private List<LogEntry> getLogEntries(DynamoDbIndex<LogBatch> index, String streamId, String caseId) {
        var startBatch = new LogBatch();
        startBatch.setService(Service.LOGGER_TEST);
        startBatch.setStage(DcsmConstants.STAGE);
        startBatch.setInstance(streamId);
        startBatch.setCaseId(caseId);
        var batches = index.query(QueryConditional.sortGreaterThan(Key.builder()
                                                                       .partitionValue(startBatch.getCaseId())
                                                                       .sortValue(Instant.ofEpochMilli(0L).toString())
                                                                       .build()));
        return batches.stream().flatMap(page -> page.items().stream())
                   .filter(batch -> Objects.equals(caseId, batch.getCaseId()))
                   .flatMap(batch -> batch.getLogEntries().stream())
                   .sorted(Comparator.comparing(LogEntry::getDate).thenComparing(LogEntry::getMessage))
                   .collect(Collectors.toList());
    }

    private LogEntry buildEntry(long dateInMs, Priority priority, String message) {
        var result = new LogEntry();
        result.setDate(Instant.ofEpochMilli(dateInMs));
        result.setPriority(priority);
        result.setMessage(message);
        return result;
    }
}
