package ai.phast.ctdynamo;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConsumedCapacity;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * The result of a query. This is package private; the PaginatedResult class that this extends should be the class
 * presented to applications using this library.
 * @param <T> The type of item in the index
 */
class ScanResult<T> extends IterableResult<T, ScanResponse> {

    private final ScanRequest.Builder scanBuilder;

    ScanResult(DynamoIndex<T, ?, ?> index, ScanRequest.Builder scanBuilder, int limit) {
        super(index, limit);
        this.scanBuilder = scanBuilder;
        init();
    }

    @Override
    protected CompletableFuture<ScanResponse> fetchNextPage(Map<String, AttributeValue> exclusiveStart) {
        if (exclusiveStart != null) {
            scanBuilder.exclusiveStartKey(exclusiveStart);
        }
        var request = scanBuilder.build();
        return getIndex().getAsyncClient() == null ? CompletableFuture.supplyAsync(() -> getIndex().getClient().scan(request)) : getIndex().getAsyncClient().scan(request);
    }

    @Override
    protected int getScannedCount(ScanResponse response) {
        return response.scannedCount() == null ? 0 : response.scannedCount();
    }

    @Override
    protected int getCount(ScanResponse response) {
        return response.count() == null ? 0 : response.count();
    }

    @Override
    protected ConsumedCapacity getRawCapacity(ScanResponse response) {
        return response.consumedCapacity();
    }

    @Override
    protected Map<String, AttributeValue> getLastEvaluatedKey(ScanResponse response) {
        return response.hasLastEvaluatedKey() ? response.lastEvaluatedKey() : null;
    }

    @Override
    protected List<Map<String, AttributeValue>> getItems(ScanResponse response) {
        return response.hasItems() ? response.items() : Collections.emptyList();
    }
}
