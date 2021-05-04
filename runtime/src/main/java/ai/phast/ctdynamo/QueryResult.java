package ai.phast.ctdynamo;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConsumedCapacity;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * The result of a query. This is package private; the PaginatedResult class that this extends should be the class
 * presented to applications using this library.
 * @param <T> The type of item in the index
 */
class QueryResult<T> extends PagedResult<T, QueryResponse> {

    private final QueryRequest.Builder queryBuilder;

    QueryResult(DynamoIndex<T, ?, ?> index, QueryRequest.Builder queryBuilder, int limit) {
        super(index, limit);
        this.queryBuilder = queryBuilder;
        init();
    }

    @Override
    CompletableFuture<QueryResponse> fetchNextPage(Map<String, AttributeValue> exclusiveStart) {
        if (exclusiveStart != null) {
            queryBuilder.exclusiveStartKey(exclusiveStart);
        }
        var request = queryBuilder.build();
        return getIndex().getAsyncClient() == null ? CompletableFuture.supplyAsync(() -> getIndex().getClient().query(request)) : getIndex().getAsyncClient().query(request);
    }

    @Override
    int getScannedCount(QueryResponse response) {
        return response.scannedCount() == null ? 0 : response.scannedCount();
    }

    @Override
    int getCount(QueryResponse response) {
        return response.count() == null ? 0 : response.count();
    }

    @Override
    ConsumedCapacity getRawCapacity(QueryResponse response) {
        return response.consumedCapacity();
    }

    @Override
    Map<String, AttributeValue> getLastEvaluatedKey(QueryResponse response) {
        return response.hasLastEvaluatedKey() ? response.lastEvaluatedKey() : null;
    }

    @Override
    List<Map<String, AttributeValue>> getItems(QueryResponse response) {
        return response.hasItems() ? response.items() : Collections.emptyList();
    }
}
