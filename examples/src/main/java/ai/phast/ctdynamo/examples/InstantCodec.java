package ai.phast.ctdynamo.examples;

import ai.phast.ctdynamo.DynamoCodec;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.time.Instant;

public class InstantCodec extends DynamoCodec<Instant> {
    @Override
    public AttributeValue encode(Instant value) {
        return AttributeValue.builder().s(value.toString()).build();
    }

    @Override
    public Instant decode(AttributeValue dynamoValue) {
        return Instant.parse(dynamoValue.s());
    }
}
