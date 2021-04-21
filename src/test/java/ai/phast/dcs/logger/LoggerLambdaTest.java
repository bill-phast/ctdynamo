package ai.phast.dcs.logger;

import ai.phast.dcsm.shared.JsonMapper;
import ai.phast.dcsm.shared.Log;
import ai.phast.dcsm.shared.logDb.LogBatch;
import ai.phast.dcsm.shared.logDb.LogEntry;
import ai.phast.dcsm.shared.logDb.LogInstance;
import ai.phast.dcsm.shared.logDb.Priority;
import ai.phast.dcsm.shared.logDb.Service;
import ai.phast.dcsm.shared.mocks.MockAsyncTable;
import ai.phast.dcsm.shared.mocks.MockContext;
import ai.phast.dcsm.shared.mocks.MockLambdaLogger;
import lombok.EqualsAndHashCode;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.enhanced.dynamodb.model.PutItemEnhancedRequest;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.utils.StringInputStream;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.zip.GZIPOutputStream;

@EqualsAndHashCode
public class LoggerLambdaTest {

    /**
     * This is the lambda's log retention, in seconds. We compute another way here just to avoid the "test that the code you wrote
     * is what you wrote" trap. If we ever change the constant in the lambda, tests will fail unless this is update also.
     */
    private static final long RETENTION_SECS = 365 * 24 * 60 * 60;

    private final MockAsyncTable<LogBatch> dcsBatchesTable = new MockAsyncTable<>(LogBatch.class);
    private final MockAsyncTable<LogInstance> dcsInstancesTable = new MockAsyncTable<>(LogInstance.class);
    private final MockAsyncTable<LogBatch> dcsmBatchesTable = new MockAsyncTable<>(LogBatch.class);
    private final MockAsyncTable<LogInstance> dcsmInstancesTable = new MockAsyncTable<>(LogInstance.class);

    @BeforeAll
    public static void setup() {
        // In the IDE these will not be set so our lambda refuses to start up unless we set them here.
        if (System.getenv("AWS_REGION") == null) {
            System.setProperty("aws.region", "us-east-2");
        }
        System.setProperty("STAGE", "test");
        System.setProperty("DCSM_ROLE_ARN", "arn:aws:fake:arn");
        System.setProperty("DCSM_REGION", "us-east-1");
    }

    @Test
    public void testHandleRequest_shouldPutBatch_fromRawCloudwatchInput() throws Exception {
        // Setup cloudwatch event batch
        var event = buildCloudwatchLogEvent("[ERROR][HULK-100]: This is an error in case HULK-100",
                Instant.parse("2018-11-30T18:35:24.00Z").toEpochMilli());
        var cloudwatchLogEventBatch = buildCloudWatchLogEventBatch("bill-dcs-logger123Test", "xyz12345", List.of(event));

        // Setup log batches
        var logEntry = buildLogEntry(Priority.ERROR, "2018-11-30T18:35:24.00Z", "This is an error in case HULK-100");
        var logBatch = buildLogBatch(Service.UNKNOWN, "bill", "xyz12345", "HULK-100", List.of(logEntry));

        // Setup
        var eventBatchJson = JsonMapper.get().asString(cloudwatchLogEventBatch);
        var encodedData = new String(Base64.getEncoder().encode(encodeWithGZIP(eventBatchJson)));

        var lambda = buildLambda(null, false);

        var awsLogsRequest = new AwsLogsRequest();
        var awsLogs = new AwsLogsRequest.AwsLogs();
        awsLogs.setData(encodedData);
        awsLogsRequest.setAwslogs(awsLogs);
        var lambdaReq = JsonMapper.get().asString(awsLogsRequest);

        // Act
        lambda.handleRequest(new StringInputStream(lambdaReq), new ByteArrayOutputStream(), new MockContext());

        // Verify
        dcsBatchesTable.verifyContainsExactly(logBatch);
        dcsmBatchesTable.verifyContainsExactly(logBatch);
    }

    @Test
    public void testHandleRequest_shouldPutBatch_fromRawCloudwatchInputWithPii() throws Exception {
        // Setup cloudwatch event batch
        var event1 = buildCloudwatchLogEvent("[ERROR][HULK-100]: This is an error in case HULK-100 for {patient=Deadpool}",
            Instant.parse("2018-11-30T18:35:24.00Z").toEpochMilli());
        var event2 = buildCloudwatchLogEvent("[INFO][HULK-100]: This is info {address=1600 Penn Ave} for {who=Mr. President}",
            Instant.parse("2018-11-30T18:35:25.00Z").toEpochMilli());
        var event3 = buildCloudwatchLogEvent("Stuff has gone wrong",
            Instant.parse("2018-11-30T18:35:26.00Z").toEpochMilli());
        var cloudwatchLogEventBatch = buildCloudWatchLogEventBatch("bill-dcs-device", "xyz12345",
            List.of(event1, event2, event3));

        // Setup log batches
        var piiLogBatch = buildLogBatch(Service.DEVICE, "bill", "xyz12345", "HULK-100", List.of(
            buildLogEntry(Priority.ERROR, "2018-11-30T18:35:24.00Z", "This is an error in case HULK-100 for {patient=Deadpool}"),
            buildLogEntry(Priority.INFO, "2018-11-30T18:35:25.00Z", "This is info {address=1600 Penn Ave} for {who=Mr. President}")));
        var sanitizedLogBatch = buildLogBatch(Service.DEVICE, "bill", "xyz12345", "HULK-100", List.of(
            buildLogEntry(Priority.ERROR, "2018-11-30T18:35:24.00Z", "This is an error in case HULK-100 for {patient}"),
            buildLogEntry(Priority.INFO, "2018-11-30T18:35:25.00Z", "This is info {address} for {who}")));
        var cleanLogBatch = buildLogBatch(Service.DEVICE, "bill", "xyz12345", null, List.of(
            buildLogEntry(Priority.INFO, "2018-11-30T18:35:26.00Z", "Stuff has gone wrong")));


        // Setup
        var eventBatchJson = JsonMapper.get().asString(cloudwatchLogEventBatch);
        var encodedData = new String(Base64.getEncoder().encode(encodeWithGZIP(eventBatchJson)));

        var lambda = buildLambda();

        var awsLogsRequest = new AwsLogsRequest();
        var awsLogs = new AwsLogsRequest.AwsLogs();
        awsLogs.setData(encodedData);
        awsLogsRequest.setAwslogs(awsLogs);
        var lambdaReq = JsonMapper.get().asString(awsLogsRequest);

        // Act
        lambda.handleRequest(new StringInputStream(lambdaReq), new ByteArrayOutputStream(), new MockContext());

        // Verify
        dcsBatchesTable.verifyContainsExactly(piiLogBatch, cleanLogBatch);
        dcsmBatchesTable.verifyContainsExactly(sanitizedLogBatch, cleanLogBatch);
    }

    @Test
    public void testHandleRequest_shouldPutBatch_whenNoExistingLogBatch() {

        // Setup
        var lambda = buildLambda();

        // Setup cloudwatch event batch
        var event = buildCloudwatchLogEvent("[ERROR][HULK-100]: This is an error in case HULK-100",
                Instant.parse("2018-11-30T18:35:24.00Z").toEpochMilli());
        var cloudwatchLogEventBatch = buildCloudWatchLogEventBatch("prod-dcs-logger123Test", "abc4321", List.of(event));

        // Setup log batches
        var logEntry = buildLogEntry(Priority.ERROR, "2018-11-30T18:35:24.00Z", "This is an error in case HULK-100");
        var logBatch = buildLogBatch(Service.UNKNOWN, "prod", "abc4321", "HULK-100", List.of(logEntry));

        // Act
        lambda.handleRequest(cloudwatchLogEventBatch, new MockContext(), new Log(new MockLambdaLogger()));

        // Verify
        dcsBatchesTable.verifyContainsExactly(logBatch);
        dcsmBatchesTable.verifyContainsExactly(logBatch);
    }

    @Test
    public void testHandleRequest_shouldMergeExistingBatch_whenHaveExistingLogBatch() {
        // Setup
        var logEntry1 = buildLogEntry(Priority.ERROR, "2018-11-30T18:35:24.00Z", "This is an error in case HULK-100");
        var logEntry2 = buildLogEntry(Priority.INFO, "2018-11-30T18:35:24.00Z", "Message from runtime");
        // Because our log batches that we pull from the database get mutated, we need two separate batches for the two tables. Otherwise we'll mutate
        // the same batch twice.
        var existingDcsLogBatch = buildLogBatch(Service.LOGGER_TEST, "prod", "xyz12345", null, new ArrayList<>(List.of(logEntry1)));
        var existingDcsmLogBatch = buildLogBatch(Service.LOGGER_TEST, "prod", "xyz12345", null, new ArrayList<>(List.of(logEntry1)));
        dcsBatchesTable.addAll(existingDcsLogBatch);
        dcsmBatchesTable.addAll(existingDcsmLogBatch);

        // Setup cloudwatch event batches
        var event = buildCloudwatchLogEvent("Message from runtime",
                Instant.parse("2018-11-30T18:35:24.00Z").toEpochMilli());
        var cloudwatchLogEventBatch = buildCloudWatchLogEventBatch("lambda/prod-dcs-loggerTest", "xyz12345", List.of(event));
        var lambda = buildLambda();

        // Act
        lambda.handleRequest(cloudwatchLogEventBatch, new MockContext(), new Log(new MockLambdaLogger()));

        // Verify
        // make sure put was done that combines messages
        var logBatchCombined = buildLogBatch(Service.LOGGER_TEST, "prod", "xyz12345", null, List.of(logEntry1, logEntry2));
        dcsBatchesTable.verifyContainsExactly(logBatchCombined);
        dcsmBatchesTable.verifyContainsExactly(logBatchCombined);
    }

    @Test
    public void testHandleRequest_shouldCreateLogInstance_whenHaveVersionMessage() {
        // Setup
        var lambda = buildLambda();

        // Setup cloudwatch event batch
        var event = buildCloudwatchLogEvent("DCS Versions: 123",
                Instant.parse("2018-11-30T18:35:24.00Z").toEpochMilli());
        var cloudwatchLogEventBatch = buildCloudWatchLogEventBatch("bill-dcs-loggerTest", "abc4321", List.of(event));

        // Setup log batches
        var logEntry = buildLogEntry(Priority.INFO, "2018-11-30T18:35:24.00Z", "DCS Versions: 123");
        var logBatch = buildLogBatch(Service.LOGGER_TEST, "bill", "abc4321", null, List.of(logEntry));

        // Setup log instance
        var logInstance = buildLogInstance(Service.LOGGER_TEST, "bill", "abc4321", "2018-11-30T18:35:24.00Z");

        // Act
        lambda.handleRequest(cloudwatchLogEventBatch, new MockContext(), new Log(new MockLambdaLogger()));

        // Verify
        dcsBatchesTable.verifyContainsExactly(logBatch);
        dcsmBatchesTable.verifyContainsExactly(logBatch);
        dcsInstancesTable.verifyContainsExactly(logInstance);
        dcsmInstancesTable.verifyContainsExactly(logInstance);
    }

    @Test
    public void testHandleRequest_shouldSkipCreateLogInstance_whenNoVersionMessage() {
        // Setup
        var lambda = buildLambda();

        // Setup cloudwatch event batch
        var event = buildCloudwatchLogEvent("[ERROR][HULK-100]: HULK-100 has error.",
                Instant.parse("2018-11-30T18:35:24.00Z").toEpochMilli());
        var cloudwatchLogEventBatch = buildCloudWatchLogEventBatch("bill-dcs-loggerTest", "abc4321", List.of(event));

        // Act
        lambda.handleRequest(cloudwatchLogEventBatch, new MockContext(), new Log(new MockLambdaLogger()));

        // Verify
        dcsInstancesTable.verifyContainsExactly();
        dcsmInstancesTable.verifyContainsExactly();
    }

    @Test
    public void testHandleRequest_shouldThrow_whenInputInvalid() {
        // Setup
        var lambda = buildLambda();

        // Act and verify
        Assertions.assertThrows(IOException.class,
            () -> lambda.handleRequest(new StringInputStream("La la la"), new ByteArrayOutputStream(), new MockContext()));
    }

    @Test
    public void testBuildLogBatchesAndUpload_shouldUploadTwo_whenHaveThreeBatches() throws Exception {

        // Setup
        var loggerLambda = buildLambda();

        // Setup cloudwatch event batch
        var event1 = buildCloudwatchLogEvent("[ERROR][HULK-100]: This is an error in case HULK-100",
                Instant.parse("2000-11-30T18:35:24.00Z").toEpochMilli());
        var event2 = buildCloudwatchLogEvent("[ERROR]: This is an error",
                Instant.parse("2018-11-30T19:35:24.00Z").toEpochMilli());
        var event3 = buildCloudwatchLogEvent("Message from runtime",
                Instant.parse("2021-11-30T18:35:24.00Z").toEpochMilli());
        var event4 = buildCloudwatchLogEvent("[INFO][SAD-123]: Case SAD-123 too sad",
                Instant.parse("2030-11-30T18:35:24.00Z").toEpochMilli());
        var event5 = buildCloudwatchLogEvent("[WARN]: This is a warning",
                Instant.parse("2040-11-30T20:35:24.00Z").toEpochMilli());

        var cloudwatchLogEventBatch = buildCloudWatchLogEventBatch("Foo/bill-dcs-loggerTest", "xyz12345", List.of(event1, event2, event3, event4, event5));

        // Setup log batches
        var logEntry1 = buildLogEntry(Priority.ERROR, "2018-11-30T19:35:24.00Z", "This is an error");
        var logEntry2 = buildLogEntry(Priority.INFO, "2021-11-30T18:35:24.00Z", "Message from runtime");
        var logEntry3 = buildLogEntry(Priority.ERROR, "2000-11-30T18:35:24.00Z", "This is an error in case HULK-100");
        var logEntry4 = buildLogEntry(Priority.INFO, "2030-11-30T18:35:24.00Z", "Case SAD-123 too sad");
        var logEntry5 = buildLogEntry(Priority.WARN, "2040-11-30T20:35:24.00Z", "This is a warning");

        var logBatchWithoutCaseId = buildLogBatch(Service.LOGGER_TEST, "bill", "xyz12345", null, List.of(logEntry1, logEntry2, logEntry5));
        var logBatchWithHulk = buildLogBatch(Service.LOGGER_TEST, "bill", "xyz12345", "HULK-100", List.of(logEntry3));
        var logBatchWithSad = buildLogBatch(Service.LOGGER_TEST, "bill", "xyz12345", "SAD-123", List.of(logEntry4));

        // Act
        var futures = loggerLambda.buildLogBatchesAndUpload(cloudwatchLogEventBatch, new Log(new MockLambdaLogger()));
        futures.forEach(CompletableFuture::join);

        // Verify
        dcsBatchesTable.verifyContainsExactly(logBatchWithHulk, logBatchWithSad, logBatchWithoutCaseId);
        dcsmBatchesTable.verifyContainsExactly(logBatchWithHulk, logBatchWithSad, logBatchWithoutCaseId);
    }

    @Test
    public void testBuildLogBatchesAndUpload_shouldPrintLog_whenUnknownService() {

        // Setup
        var loggerLambda = buildLambda();
        var mockLog = new MockLambdaLogger();
        var log = new Log(mockLog);

        // Setup cloudwatch event batch
        var event = buildCloudwatchLogEvent("[ERROR][HULK-100]: This is an error in case HULK-100",
                Instant.parse("2018-11-30T18:35:24.00Z").toEpochMilli());

        var cloudwatchLogEventBatch = buildCloudWatchLogEventBatch("bill-dcs-logger123Test", "xyz12345", List.of(event));

        // Setup log batches
        var logEntry = buildLogEntry(Priority.ERROR, "2018-11-30T18:35:24.00Z", "This is an error in case HULK-100");
        var logBatch = buildLogBatch(Service.UNKNOWN, "bill", "xyz12345", "HULK-100", List.of(logEntry));

        // Act
        var futures = loggerLambda.buildLogBatchesAndUpload(cloudwatchLogEventBatch, log);
        futures.forEach(CompletableFuture::join);

        // Verify
        Assertions.assertTrue(mockLog.getMessages().stream().anyMatch(m -> m.startsWith("[WARN]")), "Messages: " + mockLog.getMessages());
        dcsBatchesTable.verifyContainsExactly(logBatch);
        dcsmBatchesTable.verifyContainsExactly(logBatch);
    }

    @Test
    public void testBuildLogBatchesAndUpload_shouldPrintLog_whenCorruptGroupName() {

        // Setup
        var loggerLambda = buildLambda();
        var mockLog = new MockLambdaLogger();
        var log = new Log(mockLog);

        // Setup cloudwatch event batch
        var event = buildCloudwatchLogEvent("[ERROR][HULK-100]: This is an error in case HULK-100",
            Instant.parse("2018-11-30T18:35:24.00Z").toEpochMilli());

        var cloudwatchLogEventBatch = buildCloudWatchLogEventBatch("lambda/stuff", "xyz12345", List.of(event));

        // Setup log batches
        var logEntry = buildLogEntry(Priority.ERROR, "2018-11-30T18:35:24.00Z", "This is an error in case HULK-100");
        var logBatch = buildLogBatch(Service.UNKNOWN, "unknown", "xyz12345", "HULK-100", List.of(logEntry));

        // Act
        var futures = loggerLambda.buildLogBatchesAndUpload(cloudwatchLogEventBatch, log);
        futures.forEach(CompletableFuture::join);

        // Verify
        Assertions.assertTrue(mockLog.getMessages().stream().anyMatch(m -> m.startsWith("[WARN]")));
        dcsBatchesTable.verifyContainsExactly(logBatch);
        dcsmBatchesTable.verifyContainsExactly(logBatch);
    }

    @Test
    public void testBuildLogBatchesAndUpload_shouldBePriorityInfo_whenNoPriorityInMessage() {

        // Setup
        var loggerLambda = buildLambda();

        // Setup cloudwatch event batch
        var event = buildCloudwatchLogEvent("[HULK-100]: This is an error in case HULK-100",
                Instant.parse("2018-11-30T18:35:24.00Z").toEpochMilli());

        var cloudwatchLogEventBatch = buildCloudWatchLogEventBatch("bill-dcs-loggerTest", "xyz12345", List.of(event));

        // Setup log batches
        var logEntry = buildLogEntry(Priority.INFO, "2018-11-30T18:35:24.00Z", "[HULK-100]: This is an error in case HULK-100");
        var logBatch = buildLogBatch(Service.LOGGER_TEST, "bill", "xyz12345", null, List.of(logEntry));

        // Act
        var futures = loggerLambda.buildLogBatchesAndUpload(cloudwatchLogEventBatch, new Log(new MockLambdaLogger()));
        futures.forEach(CompletableFuture::join);

        // Verify
        dcsBatchesTable.verifyContainsExactly(logBatch);
        dcsmBatchesTable.verifyContainsExactly(logBatch);
    }

    @Test
    public void testBuildLogBatches_shouldBeAllText_whenNoColonSpace() {

        // Setup
        var loggerLambda = buildLambda();

        // Setup cloudwatch event batch
        var event = buildCloudwatchLogEvent("[ERROR]This is an error in case HULK-100",
                Instant.parse("2018-11-30T18:35:24.00Z").toEpochMilli());
        var cloudwatchLogEventBatch = buildCloudWatchLogEventBatch("bill-dcs-loggerTest", "xyz12345", List.of(event));

        // Setup log batches
        var logEntry = buildLogEntry(Priority.INFO, "2018-11-30T18:35:24.00Z", "[ERROR]This is an error in case HULK-100");
        var logBatch = buildLogBatch(Service.LOGGER_TEST, "bill", "xyz12345", null, List.of(logEntry));

        // Act
        var futures = loggerLambda.buildLogBatchesAndUpload(cloudwatchLogEventBatch, new Log(new MockLambdaLogger()));
        futures.forEach(CompletableFuture::join);

        // Verify
        dcsBatchesTable.verifyContainsExactly(logBatch);
        dcsmBatchesTable.verifyContainsExactly(logBatch);
    }

    @Test
    public void testBuildLogBatchesAndUpload_shouldOnlyHaveText_whenNoLabelButColonSpace() {
        // Setup
        var loggerLambda = buildLambda();

        // Setup cloudwatch event batch
        var event = buildCloudwatchLogEvent(": This is an error in case HULK-100",
                Instant.parse("2018-11-30T18:35:24.00Z").toEpochMilli());

        var cloudwatchLogEventBatch = buildCloudWatchLogEventBatch("bill-dcs-loggerTest", "xyz12345", List.of(event));

        // Setup log batches
        var logEntry = buildLogEntry(Priority.INFO, "2018-11-30T18:35:24.00Z", ": This is an error in case HULK-100");
        var logBatch = buildLogBatch(Service.LOGGER_TEST, "bill", "xyz12345", null, List.of(logEntry));

        // Act
        var futures = loggerLambda.buildLogBatchesAndUpload(cloudwatchLogEventBatch, new Log(new MockLambdaLogger()));
        futures.forEach(CompletableFuture::join);

        // Verify
        dcsBatchesTable.verifyContainsExactly(logBatch);
        dcsmBatchesTable.verifyContainsExactly(logBatch);
    }

    @Test
    public void testUploadLogInstancesAndUpload_shouldCreateLogInstance_whenHaveVersionMessage() {

        // Setup
        var lambda = buildLambda();

        // Setup cloudwatch event batch
        var event = buildCloudwatchLogEvent("[INFO]: DCS Versions: v123",
                Instant.parse("2018-11-30T18:35:24.00Z").toEpochMilli());
        var cloudwatchLogEventBatch = buildCloudWatchLogEventBatch("bill-dcs-loggerTest", "xyz12345", List.of(event));

        var logInstance = buildLogInstance(Service.LOGGER_TEST, "bill", "xyz12345", "2018-11-30T18:35:24.00Z");

        // Act
        var futures = lambda.buildLogBatchesAndUpload(cloudwatchLogEventBatch, new Log(new MockLambdaLogger()));
        futures.forEach(CompletableFuture::join);

        // Verify
        dcsInstancesTable.verifyContainsExactly(logInstance);
        dcsmInstancesTable.verifyContainsExactly(logInstance);
    }

    @Test
    public void testHandleRequest_shouldThrow_whenExceptionInDb() {
        // Setup
        var brokenTable = new MockAsyncTable<>(LogBatch.class) {
            @Override
            public CompletableFuture<Void> putItem(PutItemEnhancedRequest<LogBatch> request) {
                return CompletableFuture.failedFuture(new NullPointerException("Oops"));
            }
        };
        var lambda = buildLambda(Map.of("dcsBatchesTable", brokenTable));
        var cloudwatchLogEventBatch = buildCloudWatchLogEventBatch("bill-dcs-logger123Test", "xyz12345",
            List.of(buildCloudwatchLogEvent("[ERROR][HULK-100]: This is an error in case HULK-100",
                Instant.parse("2018-11-30T18:35:24.00Z").toEpochMilli())));

        // Act and verify
        Assertions.assertThrows(RuntimeException.class, () -> lambda.handleRequest(cloudwatchLogEventBatch, new MockContext(),
            new Log(new MockLambdaLogger())));
    }

    @Test
    public void testHandleRequest_shouldThrowAndPut_whenOneBatchSucceedsAndOneFails() {
        // Setup
        var brokenDcsTable = new MockAsyncTable<>(LogBatch.class) {
            @Override
            public CompletableFuture<Void> putItem(PutItemEnhancedRequest<LogBatch> request) {
                return (request.item().getCaseId() == null
                        ? CompletableFuture.failedFuture(new NullPointerException("Oops"))
                        : super.putItem(request));
            }
        };
        var lambda = buildLambda(Map.of("dcsBatchesTable", brokenDcsTable));
        var cloudwatchLogEventBatch = buildCloudWatchLogEventBatch("bill-dcs-logger123Test", "xyz12345",
            List.of(
                buildCloudwatchLogEvent("[INFO]: This will fail (no case ID)",
                    Instant.parse("2018-11-30T18:35:24.00Z").toEpochMilli()),
                buildCloudwatchLogEvent("[ERROR][HULK-100]: This will work (has case ID)",
                    Instant.parse("2018-11-30T18:36:24.00Z").toEpochMilli())));

        // Act and verify
        Assertions.assertThrows(RuntimeException.class, () -> lambda.handleRequest(cloudwatchLogEventBatch, new MockContext(),
            new Log(new MockLambdaLogger())));
        var expectedDcsLogBatch = buildLogBatch(Service.UNKNOWN, "bill", "xyz12345", "HULK-100",
            List.of(buildLogEntry(Priority.ERROR, "2018-11-30T18:36:24.00Z", "This will work (has case ID)")));
        var expectedDcsmLogBatch = buildLogBatch(Service.UNKNOWN, "bill", "xyz12345", null,
            List.of(buildLogEntry(Priority.INFO, "2018-11-30T18:35:24.00Z", "This will fail (no case ID)")));
        brokenDcsTable.verifyContainsExactly(expectedDcsLogBatch);
        dcsmBatchesTable.verifyContainsExactly(expectedDcsLogBatch, expectedDcsmLogBatch);
    }

    /**
     * This tests a condition that should never happen, but the logic is complex enough that we want to make sure it
     * works anyway.
     */
    @Test
    public void testHandleRequest_shouldPutAndThrow_ifMergeEntryDisappears() {
        // Setup
        // Make a table that throws ConditionCheckFailedException even when the item is not present
        var brokenDcsTable = new MockAsyncTable<>(LogBatch.class) {
            @Override
            public CompletableFuture<Void> putItem(PutItemEnhancedRequest<LogBatch> request) {
                return CompletableFuture.failedFuture(ConditionalCheckFailedException.builder().build());
            }
        };
        var lambda = buildLambda(Map.of("dcsBatchesTable", brokenDcsTable));

        var cloudwatchLogEventBatch = buildCloudWatchLogEventBatch("bill-dcs-logger123Test", "xyz12345",
            List.of(
                buildCloudwatchLogEvent("[WARN][HULK-100]: This will work (has case ID)",
                    Instant.parse("2018-11-30T18:36:24.00Z").toEpochMilli())));

        // Act and verify
        Assertions.assertThrows(RuntimeException.class, () -> lambda.handleRequest(cloudwatchLogEventBatch, new MockContext(),
            new Log(new MockLambdaLogger())));

        // Verify that the put happened as well as the throw
        var expectedLogBatch = buildLogBatch(Service.UNKNOWN, "bill", "xyz12345", "HULK-100",
            List.of(buildLogEntry(Priority.WARN, "2018-11-30T18:36:24.00Z", "This will work (has case ID)")));
        brokenDcsTable.verifyContainsExactly(expectedLogBatch);
        dcsmBatchesTable.verifyContainsExactly(expectedLogBatch);
    }

    @Test
    public void testSanitize_shouldRemovePii_whenPiiPresent() {
        // Setup
        var rawBatch = buildLogBatch(Service.DEVICE, "dev", "xyz", "xyz-100", List.of(
            buildLogEntry(Priority.INFO, "2020-01-01T01:01:01Z", "This is {name=superman}'s records from {place=BWH}"),
            buildLogEntry(Priority.WARN, "2020-01-01T01:01:02Z", "This is a clean message")
        ));
        var lambda = buildLambda();

        // Act
        var sanitizedBatch = lambda.sanitize(rawBatch);

        // Verify
        var expectedBatch = buildLogBatch(Service.DEVICE, "dev", "xyz", "xyz-100", List.of(
            buildLogEntry(Priority.INFO, "2020-01-01T01:01:01Z", "This is {name}'s records from {place}"),
            buildLogEntry(Priority.WARN, "2020-01-01T01:01:02Z", "This is a clean message")
        ));
        Assertions.assertEquals(expectedBatch, sanitizedBatch);

        // Make sure we didn't waste time copying the already-clean message
        Assertions.assertSame(rawBatch.getLogEntries().get(1), sanitizedBatch.getLogEntries().get(1));
    }

    @Test
    public void testSanitize_shouldReturnRaw_whenNoPiiPresent() {
        // Setup
        var rawBatch = buildLogBatch(Service.DEVICE, "gamma", "xyz", "xyz-100", List.of(
            buildLogEntry(Priority.WARN, "2020-01-01T01:01:02Z", "This is a clean message")
        ));
        var lambda = buildLambda();

        // Act
        var sanitizedBatch = lambda.sanitize(rawBatch);

        // Verify
        Assertions.assertSame(rawBatch, sanitizedBatch);
    }

    @Test
    public void testBuildDcsmClient_shouldUseRandomString_whenLogStreamUnusable() {
        // Setup
        var lambda = new LoggerLambda();
        var context = new MockContext() {
            @Override
            public String getLogStreamName() {
                return "/this/wont/work/";
            }
        };
        var lambdaLogger = new MockLambdaLogger();

        // Act
        lambda.buildDcsmClient("/this/wont/work/", new Log(lambdaLogger));

        // Verify
        // It's really hard to verify that the session name is a random string. But we log a warn from buildDcsmClient
        // only when we need to create a random session, so we'll go with that.
        Assertions.assertTrue(lambdaLogger.getMessages().stream().anyMatch(s -> s.startsWith("[WARN]")));
    }

    private LogEntry buildLogEntry(Priority priority, String date, String message) {
        var logEntry = new LogEntry();
        logEntry.setPriority(priority);
        logEntry.setDate(Instant.parse(date));
        logEntry.setMessage(message);
        return logEntry;
    }

    private LogBatch buildLogBatch(Service service, String stage, String instance, String caseId, List<LogEntry> logEntries) {
        var logBatch = new LogBatch();
        logBatch.setService(service);
        logBatch.setStage(stage);
        logBatch.setInstance(instance);
        logBatch.setCaseId(caseId);
        logBatch.setLogEntries(logEntries);
        logBatch.setTtl(logEntries.stream().map(LogEntry::getDate).min(Comparator.naturalOrder()).orElseThrow().getEpochSecond() + RETENTION_SECS);
        return logBatch;
    }

    private CloudwatchLogEvent buildCloudwatchLogEvent(String message, long timestamp) {
        var cloudwatchLogEvent = new CloudwatchLogEvent();
        cloudwatchLogEvent.setMessage(message);
        cloudwatchLogEvent.setTimestamp(timestamp);
        return cloudwatchLogEvent;
    }

    private CloudwatchLogEventBatch buildCloudWatchLogEventBatch(String logGroup, String stream, List<CloudwatchLogEvent> cloudwatchLogEvents) {
        var cloudwatchLogEventBatch = new CloudwatchLogEventBatch();
        cloudwatchLogEventBatch.setLogGroup(logGroup);
        cloudwatchLogEventBatch.setLogStream(stream);
        cloudwatchLogEventBatch.setLogEvents(cloudwatchLogEvents);
        return cloudwatchLogEventBatch;
    }

    private LogInstance buildLogInstance(Service service, String stage, String instance, String dateOfFirstMessage) {
        var logInstance = new LogInstance();
        logInstance.setService(service);
        logInstance.setStage(stage);
        logInstance.setInstance(instance);
        logInstance.setDateOfFirstMessage(Instant.parse(dateOfFirstMessage));
        logInstance.setTtl(logInstance.getDateOfFirstMessage().getEpochSecond() + RETENTION_SECS);
        return logInstance;
    }

    public static byte[] encodeWithGZIP(String str) throws IOException {
        var out = new ByteArrayOutputStream();
        var gzip = new GZIPOutputStream(out);
        gzip.write(str.getBytes(StandardCharsets.UTF_8));
        gzip.close();
        return out.toByteArray();
    }

    public LoggerLambda buildLambda() {
        return buildLambda(null, true);
    }

    public LoggerLambda buildLambda(Map<String, Object> mocks) {
        return buildLambda(mocks, true);
    }

    public LoggerLambda buildLambda(Map<String, Object> mocks, boolean autobuildDcsm) {
        var defaultMocks = Map.<String, Object>of("dcsBatchesTable", dcsBatchesTable,
            "dcsInstancesTable", dcsInstancesTable,
            "dcsmBatchesTable", dcsmBatchesTable,
            "dcsmInstancesTable", dcsmInstancesTable);
        Map<String, Object> fullMocks;
        if (mocks == null) {
            fullMocks = defaultMocks;
        } else {
            fullMocks = new HashMap<>();
            fullMocks.putAll(defaultMocks);
            fullMocks.putAll(mocks);
        }
        var lambda = new LoggerLambda(fullMocks);
        if (autobuildDcsm) {
            lambda.buildDcsmClient("aaa123", new Log(new MockLambdaLogger()));
        }
        return lambda;
    }
}
