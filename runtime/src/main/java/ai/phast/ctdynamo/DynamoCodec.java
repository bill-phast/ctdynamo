package ai.phast.ctdynamo;

public abstract class DynamoCodec<PojoT, DynamoT> {

    public abstract DynamoT encode(PojoT javaValue);

    public abstract PojoT decode(DynamoT dynamoValue);
}
