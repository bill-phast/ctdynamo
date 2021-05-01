package ai.phast.ctdynamo;

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.KeysAndAttributes;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ReturnConsumedCapacity;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public abstract class DynamoTable<T, PartitionT, SortT> extends DynamoIndex<T, PartitionT, SortT> {

    private static final int MAX_ITEMS_PER_BATCH = 25;

    public DynamoTable(DynamoDbClient client, DynamoDbAsyncClient asyncClient, String tableName,
                       String partitionKeyAttribute, String sortKeyAttribute) {
        super(client, asyncClient, tableName, null, partitionKeyAttribute, sortKeyAttribute);
    }

    public final T get(T value) {
        return get(getPartitionKey(value), getSortKey(value));
    }

    public T get(PartitionT partitionValue, SortT sortValue) {
        var response = get(GetItemRequest.builder()
                               .tableName(getTableName())
                               .key(keysToMap(partitionValue, sortValue))
                               .build());
        return response.hasItem() ? decode(response.item()) : null;
    }

    public final ExtendedItemResult<T> getExtended(T value, boolean useConsistentRead) {
        return getExtended(getPartitionKey(value), getSortKey(value), useConsistentRead);
    }

    public ExtendedItemResult<T> getExtended(PartitionT partitionValue, SortT sortValue, boolean useConsistentRead) {
        var response = get(GetItemRequest.builder()
                               .tableName(getTableName())
                               .key(keysToMap(partitionValue, sortValue))
                               .returnConsumedCapacity(ReturnConsumedCapacity.INDEXES)
                               .consistentRead(useConsistentRead)
                               .build());
        return new ExtendedItemResult<>(response.hasItem() ? decode(response.item()) : null,
            response.consumedCapacity());
    }

    public final CompletableFuture<T> getAsync(T value) {
        return getAsync(getPartitionKey(value), getSortKey(value));
    }

    public CompletableFuture<T> getAsync(PartitionT partitionValue, SortT sortValue) {
        return getAsync(GetItemRequest.builder()
                            .tableName(getTableName())
                            .key(keysToMap(partitionValue, sortValue))
                            .build())
                   .thenApply(r -> r.hasItem() ? decode(r.item()) : null);
    }

    public final CompletableFuture<ExtendedItemResult<T>> getExtendedAsync(T value, boolean useConsistentRead) {
        return getExtendedAsync(getPartitionKey(value), getSortKey(value), useConsistentRead);
    }

    public CompletableFuture<ExtendedItemResult<T>> getExtendedAsync(PartitionT partitionValue, SortT sortValue, boolean useConsistentRead) {
        return getAsync(GetItemRequest.builder()
                            .tableName(getTableName())
                            .key(keysToMap(partitionValue, sortValue))
                            .returnConsumedCapacity(ReturnConsumedCapacity.INDEXES)
                            .consistentRead(useConsistentRead)
                            .build())
                   .thenApply(r -> new ExtendedItemResult<>(r.hasItem() ? decode(r.item()) : null, r.consumedCapacity()));
    }

    private GetItemResponse get(GetItemRequest request) {
        return getClient() == null ? getAsyncClient().getItem(request).join() : getClient().getItem(request);
    }

    private CompletableFuture<GetItemResponse> getAsync(GetItemRequest request) {
        return getAsyncClient() == null
               ? CompletableFuture.supplyAsync(() -> getClient().getItem(request))
               : getAsyncClient().getItem(request);
    }

    public List<T> getBatch(List<T> keys) {
        return getBatchesAndMerge(buildBatches(keys));
    }

    public List<T> getBatch(List<PartitionT> partitionKeys, List<SortT> sortKeys) {
        return getBatchesAndMerge(buildBatches(partitionKeys, sortKeys));
    }

    private List<T> getBatchesAndMerge(List<Map<String, KeysAndAttributes>> batches) {
        Function<BatchGetItemRequest, BatchGetItemResponse> doCall = (getClient() == null
                                                                      ? req -> getAsyncClient().batchGetItem(req).join()
                                                                      : req -> getClient().batchGetItem(req));
        return batches.stream()
                   .map(batch -> BatchGetItemRequest.builder().requestItems(batch).build())
                   .map(doCall)
                   .filter(BatchGetItemResponse::hasResponses)
                   .flatMap(response -> response.responses().get(getTableName()).stream())
                   .map(this::decode)
                   .collect(Collectors.toList());
    }

    public ExtendedBatchResult<T, PartitionT, SortT> getExtendedBatches(List<T> items) {
        return getExtendedBatchesAndMerge(buildBatches(items));
    }

    public ExtendedBatchResult<T, PartitionT, SortT> getExtendedBatches(List<PartitionT> partitionKeys, List<SortT> sortKeys) {
        return getExtendedBatchesAndMerge(buildBatches(partitionKeys, sortKeys));
    }

    public ExtendedBatchResult<T, PartitionT, SortT> getExtendedBatchesAndMerge(List<Map<String, KeysAndAttributes>> batches) {
        var result = new ExtendedBatchResult<T, PartitionT, SortT>();
        for (var batch : batches) {
            var request = BatchGetItemRequest.builder()
                              .requestItems(batch)
                              .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                              .build();
            updateExtendedGetBatchResult(result,
                getClient() == null ? getAsyncClient().batchGetItem(request).join() : getClient().batchGetItem(request));
        }
        return result;
    }

    public CompletableFuture<List<T>> getBatchAsync(List<T> keys) {
        return getBatchesAndMergeAsync(buildBatches(keys));
    }

    public CompletableFuture<List<T>> getBatchAsync(List<PartitionT> partitionKeys, List<SortT> sortKeys) {
        return getBatchesAndMergeAsync(buildBatches(partitionKeys, sortKeys));
    }

    private CompletableFuture<List<T>> getBatchesAndMergeAsync(List<Map<String, KeysAndAttributes>> batches) {
        BiFunction<List<T>, BatchGetItemResponse, List<T>> merger = (list, response) -> {
            if (response.hasResponses()) {
                response.responses().get(getTableName()).forEach(m -> list.add(decode(m)));
            }
            return list;
        };
        CompletableFuture<List<T>> result = CompletableFuture.completedFuture(new ArrayList<>(batches.size() * MAX_ITEMS_PER_BATCH));
        for (var batch: batches) {
            var request = BatchGetItemRequest.builder().requestItems(batch).build();
            result = result.thenCombine(
                getAsyncClient() == null ? CompletableFuture.supplyAsync(() -> getClient().batchGetItem(request))
                                         : getAsyncClient().batchGetItem(request),
                merger);
        }
        return result;
    }

    public CompletableFuture<ExtendedBatchResult<T, PartitionT, SortT>> getExtendedBatchesAsync(List<T> items) {
        return getExtendedBatchesAndMergeAsync(buildBatches(items));
    }

    public CompletableFuture<ExtendedBatchResult<T, PartitionT, SortT>> getExtendedBatchesAsync(List<PartitionT> partitionKeys, List<SortT> sortKeys) {
        return getExtendedBatchesAndMergeAsync(buildBatches(partitionKeys, sortKeys));
    }

    public CompletableFuture<ExtendedBatchResult<T, PartitionT, SortT>> getExtendedBatchesAndMergeAsync(List<Map<String, KeysAndAttributes>> batches) {
        var result = CompletableFuture.completedFuture(new ExtendedBatchResult<T, PartitionT, SortT>());
        for (var batch : batches) {
            var request = BatchGetItemRequest.builder()
                              .requestItems(batch)
                              .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                              .build();
            result = result.thenCombine(
                getAsyncClient() == null ? CompletableFuture.supplyAsync(() -> getClient().batchGetItem(request))
                                         : getAsyncClient().batchGetItem(request),
                this::updateExtendedGetBatchResult);
        }
        return result;
    }

    private ExtendedBatchResult<T, PartitionT, SortT> updateExtendedGetBatchResult(ExtendedBatchResult<T, PartitionT, SortT> result, BatchGetItemResponse response) {
        if (response.hasResponses()) {
            response.responses().get(getTableName()).stream()
                .map(this::decode)
                .forEach(item -> result.getItems().add(item));
        }
        if (response.hasConsumedCapacity()) {
            for (var cap : response.consumedCapacity()) {
                result.getCapacity().add(cap);
            }
        }
        if (response.hasUnprocessedKeys()) {
            for (var keyMap : response.unprocessedKeys().get(getTableName()).keys()) {
                result.getUnprocessedKeys().add(new ExtendedBatchResult.Key<>(getPartitionKey(keyMap.get(getPartitionKeyAttribute())),
                    getSortKey(keyMap.get(getSortKeyAttribute()))));
            }
        }
        return result;
    }

    /**
     * Build a list of maps from table name to a list of keys. Each map will have at most MAX_ITEMS_PER_BATCH items.
     * @param partitionKeys A list of partition keys
     * @param sortKeys A list of sort keys
     * @return A list of batches to submit for processing
     */
    private List<Map<String, KeysAndAttributes>> buildBatches(List<PartitionT> partitionKeys, List<SortT> sortKeys) {
        int numItems = partitionKeys.size();
        if (numItems != sortKeys.size()) {
            throw new IllegalArgumentException("Got " + numItems + " partition keys, and "
                                                   + sortKeys.size() + " sort keys. They must be equal");
        }
        var result = new ArrayList<Map<String, KeysAndAttributes>>();
        for (int offset = 0; offset < numItems; offset += MAX_ITEMS_PER_BATCH) {
            result.add(Collections.singletonMap(getTableName(),
                KeysAndAttributes.builder()
                    .keys(IntStream.range(offset, Math.min(numItems, offset + MAX_ITEMS_PER_BATCH) - 1)
                              .mapToObj(i -> keysToMap(partitionKeys.get(i), sortKeys.get(i)))
                              .collect(Collectors.toList()))
                    .build()));
        }
        return result;
    }

    public ExtendedItemResult<T> putExtended(T value) {
        var putResponse = put(PutItemRequest.builder()
                          .tableName(getTableName())
                          .item(encode(value))
                          .returnConsumedCapacity(ReturnConsumedCapacity.INDEXES)
                          .build());
        return new ExtendedItemResult<>(putResponse.hasAttributes() ? decode(putResponse.attributes()) : null, putResponse.consumedCapacity());
    }

    public CompletableFuture<T> putAsync(T value) {
        return putAsync(PutItemRequest.builder()
                            .tableName(getTableName())
                            .item(encode(value))
                            .build())
                   .thenApply(resp -> resp.hasAttributes() ? decode(resp.attributes()) : null);
    }

    public CompletableFuture<ExtendedItemResult<T>> putExtendedAsync(T value) {
        return putAsync(PutItemRequest.builder()
                            .tableName(getTableName())
                            .item(encode(value))
                            .returnConsumedCapacity(ReturnConsumedCapacity.INDEXES)
                            .build())
                   .thenApply(resp -> new ExtendedItemResult<>(
                       resp.hasAttributes() ? decode(resp.attributes()) : null, resp.consumedCapacity()));
    }

    private PutItemResponse put(PutItemRequest request) {
        return getClient() == null ? getAsyncClient().putItem(request).join() : getClient().putItem(request);
    }

    private CompletableFuture<PutItemResponse> putAsync(PutItemRequest request) {
        return getAsyncClient() == null ? CompletableFuture.supplyAsync(() -> getClient().putItem(request)) : getAsyncClient().putItem(request);
    }

    public final T delete(T value) {
        return delete(getPartitionKey(value), getSortKey(value));
    }

    public T delete(PartitionT partitionKey, SortT sortKey) {
        var deleteResponse = delete(DeleteItemRequest.builder()
                   .tableName(getTableName())
                   .key(keysToMap(partitionKey, sortKey))
                   .build());
        return deleteResponse.hasAttributes() ? decode(deleteResponse.attributes()) : null;
    }

    public final ExtendedItemResult<T> deleteExtended(T value) {
        return deleteExtended(getPartitionKey(value), getSortKey(value));
    }

    public ExtendedItemResult<T> deleteExtended(PartitionT partitionKey, SortT sortKey) {
        var deleteResponse = delete(DeleteItemRequest.builder()
                                        .tableName(getTableName())
                                        .key(keysToMap(partitionKey, sortKey))
                                        .returnConsumedCapacity(ReturnConsumedCapacity.INDEXES)
                                        .build());
        return new ExtendedItemResult<>(deleteResponse.hasAttributes() ? decode(deleteResponse.attributes()) : null, deleteResponse.consumedCapacity());
    }
    
    public final CompletableFuture<T> deleteAsync(T value) {
        return deleteAsync(getPartitionKey(value), getSortKey(value));
    }

    public CompletableFuture<T> deleteAsync(PartitionT partitionKey, SortT sortKey) {
        return deleteAsync(DeleteItemRequest.builder()
                               .tableName(getTableName())
                               .key(keysToMap(partitionKey, sortKey))
                               .build())
                   .thenApply(resp -> resp.hasAttributes() ? decode(resp.attributes()) : null);
    }

    public final CompletableFuture<ExtendedItemResult<T>> deleteExtendedAsync(T value) {
        return deleteExtendedAsync(getPartitionKey(value), getSortKey(value));
    }

    public CompletableFuture<ExtendedItemResult<T>> deleteExtendedAsync(PartitionT partitionKey, SortT sortKey) {
        return deleteAsync(DeleteItemRequest.builder()
                               .tableName(getTableName())
                               .key(keysToMap(partitionKey, sortKey))
                               .returnConsumedCapacity(ReturnConsumedCapacity.INDEXES)
                               .build())
                   .thenApply(resp -> new ExtendedItemResult<>(
                       resp.hasAttributes() ? decode(resp.attributes()) : null, resp.consumedCapacity()));
    }

    private DeleteItemResponse delete(DeleteItemRequest request) {
        return getClient() == null ? getAsyncClient().deleteItem(request).join() : getClient().deleteItem(request);
    }

    private CompletableFuture<DeleteItemResponse> deleteAsync(DeleteItemRequest request) {
        return getAsyncClient() == null ? CompletableFuture.supplyAsync(() -> getClient().deleteItem(request)) : getAsyncClient().deleteItem(request);
    }

    public abstract <SecondaryPartitionT, SecondarySortT> DynamoIndex<T, SecondaryPartitionT, SecondarySortT> getIndex(
        String name, Class<SecondaryPartitionT> secondaryPartitionClass, Class<SecondarySortT> secondarySortClass);

    /**
     * Build a list of maps from table name to a list of keys. Each map will have at most MAX_ITEMS_PER_BATCH items.
     * @param items A list of items to use as sort keys
     * @return A list of batches to submit for processing
     */
    private List<Map<String, KeysAndAttributes>> buildBatches(List<T> items) {
        int numItems = items.size();
        var result = new ArrayList<Map<String, KeysAndAttributes>>();
        for (int offset = 0; offset < numItems; offset += MAX_ITEMS_PER_BATCH) {
            result.add(Collections.singletonMap(getTableName(),
                KeysAndAttributes.builder()
                    .keys(items.subList(offset, Math.min(numItems, offset + MAX_ITEMS_PER_BATCH)).stream()
                              .map(item -> keysToMap(getPartitionKey(item), getSortKey(item)))
                              .collect(Collectors.toList()))
                    .build()));
        }
        return result;
    }

    public abstract Map<String, AttributeValue> encode(T value);

    public abstract PartitionT getPartitionKey(T value);

    public abstract SortT getSortKey(T value);

    protected abstract PartitionT getPartitionKey(AttributeValue value);

    protected abstract SortT getSortKey(AttributeValue value);

    /**
     * Convert the partition and sort keys to a map.
     * @param partitionValue The value of the partition key
     * @param sortValue The value of the sort key. Must be null if there is no sort key
     * @return A dynamo-friendly map of attribute values
     */
    protected final Map<String, AttributeValue> keysToMap(PartitionT partitionValue, SortT sortValue) {
        return sortValue == null
               ? Collections.singletonMap(getPartitionKeyAttribute(), partitionValueToAttributeValue(partitionValue))
               : Map.of(getPartitionKeyAttribute(), partitionValueToAttributeValue(partitionValue),
                   getSortKeyAttribute(), sortValueToAttributeValue(sortValue));
    }
}
