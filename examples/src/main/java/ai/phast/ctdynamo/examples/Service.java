package ai.phast.ctdynamo.examples;

/**
 * Represents the service of logs.
 */
public enum Service {

    /** The logger service.  */
    LOGGER("logger"),

    /** Logs from the device. Instance ID is the device's serial number. */
    DEVICE("device"),

    /** The integration test of logger. */
    LOGGER_TEST("loggerTest"),

    /** The unknown type of service. */
    UNKNOWN("unknown");

    /** The name for the service as it appears in cloudwatch. */
    public final String name;

    /**
     * The constructor.
     * @param name The name of the service as it appears in cloudwatch log groups
     */
    Service(String name) {
        this.name = name;
    }
}