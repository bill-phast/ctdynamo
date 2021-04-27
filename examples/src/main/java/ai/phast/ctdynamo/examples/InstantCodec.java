package ai.phast.ctdynamo.examples;

import ai.phast.ctdynamo.DynamoCodec;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Locale;

public class InstantCodec extends DynamoCodec<Instant> {

    public static final DateTimeFormatter INSTANT_FORMATTER = new DateTimeFormatterBuilder().appendInstant(3).toFormatter(Locale.US);

    @Override
    public AttributeValue encode(Instant value) {
        return AttributeValue.builder().s(INSTANT_FORMATTER.format(value)).build();
    }

    @Override
    public Instant decode(AttributeValue dynamoValue) {
        return Instant.parse(dynamoValue.s());
    }
}
