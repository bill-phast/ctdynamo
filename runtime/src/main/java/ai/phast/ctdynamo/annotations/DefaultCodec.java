package ai.phast.ctdynamo.annotations;

import ai.phast.ctdynamo.DynamoCodec;

/**
 * A nonfuctional placeholder codec. This is the default for all attributes that let you specify a codec. If it is
 * present, then we should use the default codec.
 *
 * <p>Package protected because it should not be visible to applications.
 */
class DefaultCodec extends DynamoCodec<Void, Void> {

    @Override
    public Void encode(Void javaValue) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Void decode(Void dynamoValue) {
        throw new UnsupportedOperationException();
    }
}
