package ai.phast.ctdynamo.annotations;

import ai.phast.ctdynamo.DynamoCodec;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * A nonfuctional placeholder codec. This is the default for all attributes that let you specify a codec. If it is
 * present, then we should use the default codec.
 *
 * <p>Package protected because it should not be visible to applications.
 */
class DefaultCodec extends DynamoCodec<Void> {

    @Override
    public AttributeValue encode(Void value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Void decode(AttributeValue dynamoValue) {
        throw new UnsupportedOperationException();
    }
}
