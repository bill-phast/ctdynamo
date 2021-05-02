package ai.phast.ctdynamo;

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.DeleteRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.KeysAndAttributes;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.ReturnConsumedCapacity;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

public abstract class DynamoTable<T, PartitionT, SortT> extends DynamoIndex<T, PartitionT, SortT> {

    private static final int MAX_ITEMS_PER_BATCH = 25;

    public DynamoTable(DynamoDbClient client, DynamoDbAsyncClient asyncClient, String tableName,
                       String partitionKeyAttribute, String sortKeyAttribute) {
        super(client, asyncClient, tableName, null, partitionKeyAttribute, sortKeyAttribute);
    }

    public final T getItem(T value) {
        return getItem(getPartitionKey(value), getSortKey(value));
    }

    public final T getItem(Key<PartitionT, SortT> key) {
        return getItem(key.getPartition(), key.getSort());
    }

    public T getItem(PartitionT partitionValue, SortT sortValue) {
        var response = getItem(GetItemRequest.builder()
                               .tableName(getTableName())
                               .key(keysToMap(partitionValue, sortValue))
                               .build());
        return response.hasItem() ? decode(response.item()) : null;
    }

    public final ExtendedItemResult<T> getItemExtended(T value, boolean useConsistentRead) {
        return getItemExtended(getPartitionKey(value), getSortKey(value), useConsistentRead);
    }

    public final ExtendedItemResult<T> getItemExtended(Key<PartitionT, SortT> key, boolean useConsistentRead) {
        return getItemExtended(key.getPartition(), key.getSort(), useConsistentRead);
    }

    public ExtendedItemResult<T> getItemExtended(PartitionT partitionValue, SortT sortValue, boolean useConsistentRead) {
        var response = getItem(GetItemRequest.builder()
                               .tableName(getTableName())
                               .key(keysToMap(partitionValue, sortValue))
                               .returnConsumedCapacity(ReturnConsumedCapacity.INDEXES)
                               .consistentRead(useConsistentRead)
                               .build());
        return new ExtendedItemResult<>(response.hasItem() ? decode(response.item()) : null,
            response.consumedCapacity());
    }

    private GetItemResponse getItem(GetItemRequest request) {
        return getClient() == null ? getAsyncClient().getItem(request).join() : getClient().getItem(request);
    }

    public final CompletableFuture<T> getItemAsync(T value) {
        return getItemAsync(getPartitionKey(value), getSortKey(value));
    }

    public final CompletableFuture<T> getItemAsync(Key<PartitionT, SortT> key) {
        return getItemAsync(key.getPartition(), key.getSort());
    }

    public CompletableFuture<T> getItemAsync(PartitionT partitionValue, SortT sortValue) {
        return getItemAsync(GetItemRequest.builder()
                            .tableName(getTableName())
                            .key(keysToMap(partitionValue, sortValue))
                            .build())
                   .thenApply(r -> r.hasItem() ? decode(r.item()) : null);
    }

    public final CompletableFuture<ExtendedItemResult<T>> getItemExtendedAsync(T value, boolean useConsistentRead) {
        return getItemExtendedAsync(getPartitionKey(value), getSortKey(value), useConsistentRead);
    }

    public final CompletableFuture<ExtendedItemResult<T>> getItemExtendedAsync(Key<PartitionT, SortT> key, boolean useConsistentRead) {
        return getItemExtendedAsync(key.getPartition(), key.getSort(), useConsistentRead);
    }

    public CompletableFuture<ExtendedItemResult<T>> getItemExtendedAsync(PartitionT partitionValue, SortT sortValue, boolean useConsistentRead) {
        return getItemAsync(GetItemRequest.builder()
                            .tableName(getTableName())
                            .key(keysToMap(partitionValue, sortValue))
                            .returnConsumedCapacity(ReturnConsumedCapacity.INDEXES)
                            .consistentRead(useConsistentRead)
                            .build())
                   .thenApply(r -> new ExtendedItemResult<>(r.hasItem() ? decode(r.item()) : null, r.consumedCapacity()));
    }

    private CompletableFuture<GetItemResponse> getItemAsync(GetItemRequest request) {
        return getAsyncClient() == null
               ? CompletableFuture.supplyAsync(() -> getClient().getItem(request))
               : getAsyncClient().getItem(request);
    }

    /**
     * Get a batch of items. This can't be named "getBatch" because erasure makes it the same as the by-key version.
     * @param items The items whose keys will be used to get the batch
     * @return The result of the read
     */
    public List<T> getBatchByItem(List<T> items) {
        return getBatch(buildGetBatchesFromItems(items));
    }

    /**
     * Get a batch of items. This can't be named "getBatch" because erasure makes it the same as the by-item version.
     * @param keys The keys used to get the batch
     * @return The result of the read
     */
    public List<T> getBatchByKey(List<Key<PartitionT, SortT>> keys) {
        return getBatch(buildGetBatchesFromKeys(keys));
    }

    private List<T> getBatch(List<Map<String, KeysAndAttributes>> batches) {
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

    public ExtendedBatchResult<T, Key<PartitionT, SortT>> getBatchByItemExtended(List<T> items) {
        return getBatchExtended(buildGetBatchesFromItems(items));
    }

    public ExtendedBatchResult<T, Key<PartitionT, SortT>> getBatchByKeyExtended(List<Key<PartitionT, SortT>> keys) {
        return getBatchExtended(buildGetBatchesFromKeys(keys));
    }

    private ExtendedBatchResult<T, Key<PartitionT, SortT>> getBatchExtended(List<Map<String, KeysAndAttributes>> batches) {
        var result = new ExtendedBatchResult<T, Key<PartitionT, SortT>>();
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

    public CompletableFuture<List<T>> getBatchByItemAsync(List<T> items) {
        return getBatchAsync(buildGetBatchesFromItems(items));
    }

    public CompletableFuture<List<T>> getBatchByKeyAsync(List<Key<PartitionT, SortT>> keys) {
        return getBatchAsync(buildGetBatchesFromKeys(keys));
    }

    private CompletableFuture<List<T>> getBatchAsync(List<Map<String, KeysAndAttributes>> batches) {
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

    public CompletableFuture<ExtendedBatchResult<T, Key<PartitionT, SortT>>> getBatchByItemExtendedAsync(List<T> items) {
        return getBatchExtendedAsync(buildGetBatchesFromItems(items));
    }

    public CompletableFuture<ExtendedBatchResult<T, Key<PartitionT, SortT>>> getBatchByKeyExtendedAsync(List<Key<PartitionT, SortT>> keys) {
        return getBatchExtendedAsync(buildGetBatchesFromKeys(keys));
    }

    public CompletableFuture<ExtendedBatchResult<T, Key<PartitionT, SortT>>> getBatchExtendedAsync(List<Map<String, KeysAndAttributes>> batches) {
        var result = CompletableFuture.completedFuture(new ExtendedBatchResult<T, Key<PartitionT, SortT>>());
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

    private ExtendedBatchResult<T, Key<PartitionT, SortT>> updateExtendedGetBatchResult(ExtendedBatchResult<T, Key<PartitionT, SortT>> result, BatchGetItemResponse response) {
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
                result.getUnprocessedValues().add(new Key<>(getPartitionKey(keyMap.get(getPartitionKeyAttribute())),
                    getSortKey(keyMap.get(getSortKeyAttribute()))));
            }
        }
        return result;
    }

    /**
     * Build a list of maps from table name to a list of keys. Each map will have at most MAX_ITEMS_PER_BATCH items.
     * @param items A list of items to use as sort keys
     * @return A list of batches to submit for processing
     */
    private List<Map<String, KeysAndAttributes>> buildGetBatchesFromItems(List<T> items) {
        int numItems = items.size();
        var result = new ArrayList<Map<String, KeysAndAttributes>>((numItems + MAX_ITEMS_PER_BATCH - 1) / MAX_ITEMS_PER_BATCH);
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

    /**
     * Build a list of maps from table name to a list of keys. Each map will have at most MAX_ITEMS_PER_BATCH items.
     * @param keys A list of keys
     * @return A list of batches to submit for processing
     */
    private List<Map<String, KeysAndAttributes>> buildGetBatchesFromKeys(List<Key<PartitionT, SortT>> keys) {
        int numItems = keys.size();
        var result = new ArrayList<Map<String, KeysAndAttributes>>((numItems + MAX_ITEMS_PER_BATCH - 1) / MAX_ITEMS_PER_BATCH);
        for (int offset = 0; offset < numItems; offset += MAX_ITEMS_PER_BATCH) {
            result.add(Collections.singletonMap(getTableName(),
                KeysAndAttributes.builder()
                    .keys(keys.subList(offset, Math.min(numItems, offset + MAX_ITEMS_PER_BATCH)).stream()
                              .map(key -> keysToMap(key.getPartition(), key.getSort()))
                              .collect(Collectors.toList()))
                    .build()));
        }
        return result;
    }

    public T putItem(T value) {
        var putResponse = putItem(PutItemRequest.builder()
                                      .tableName(getTableName())
                                      .item(encode(value))
                                      .build());
        return putResponse.hasAttributes() ? decode(putResponse.attributes()) : null;
    }

    public void putBatch(List<T> values) {
        int numValues = values.size();
        for (int i = 0; i < numValues; i += MAX_ITEMS_PER_BATCH) {
            var request = BatchWriteItemRequest.builder()
                              .requestItems(Collections.singletonMap(getTableName(),
                                  values.subList(i, Math.min(numValues, i + MAX_ITEMS_PER_BATCH)).stream()
                                      .map(value -> WriteRequest.builder().putRequest(PutRequest.builder()
                                                                                          .item(encode(value)).build()).build())
                                      .collect(Collectors.toList())))
                              .build();
            if (getClient() == null) {
                getAsyncClient().batchWriteItem(request).join();
            } else {
                getClient().batchWriteItem(request);
            }
        }
    }

    public ExtendedItemResult<T> putItemExtended(T value) {
        var putResponse = putItem(PutItemRequest.builder()
                          .tableName(getTableName())
                          .item(encode(value))
                          .returnConsumedCapacity(ReturnConsumedCapacity.INDEXES)
                          .build());
        return new ExtendedItemResult<>(putResponse.hasAttributes() ? decode(putResponse.attributes()) : null, putResponse.consumedCapacity());
    }

    public ExtendedBatchResult<T, T> putBatchExtended(List<T> values) {
        int numValues = values.size();
        var result = new ExtendedBatchResult<T, T>();
        for (int i = 0; i < numValues; i += MAX_ITEMS_PER_BATCH) {
            var request = BatchWriteItemRequest.builder()
                              .requestItems(Collections.singletonMap(getTableName(),
                                  values.subList(i, Math.min(numValues, i + MAX_ITEMS_PER_BATCH)).stream()
                                      .map(value -> WriteRequest.builder().putRequest(PutRequest.builder()
                                                                                          .item(encode(value)).build()).build())
                                      .collect(Collectors.toList())))
                              .build();
            var response = getClient() == null ? getAsyncClient().batchWriteItem(request).join() : getClient().batchWriteItem(request);
            updateBatchResultForPut(result, response);
        }
        return result;
    }

    public CompletableFuture<T> putItemAsync(T value) {
        return putItemAsync(PutItemRequest.builder()
                            .tableName(getTableName())
                            .item(encode(value))
                            .build())
                   .thenApply(resp -> resp.hasAttributes() ? decode(resp.attributes()) : null);
    }

    public CompletableFuture<Void> putBatchAsync(List<T> values) {
        int numValues = values.size();
        var futures = new CompletableFuture<?>[(numValues + MAX_ITEMS_PER_BATCH) / MAX_ITEMS_PER_BATCH];
        for (int i = 0; i < numValues; i += MAX_ITEMS_PER_BATCH) {
            var request = BatchWriteItemRequest.builder()
                              .requestItems(Collections.singletonMap(getTableName(),
                                  values.subList(i, Math.min(numValues, i + MAX_ITEMS_PER_BATCH)).stream()
                                      .map(value -> WriteRequest.builder().putRequest(PutRequest.builder()
                                                                                          .item(encode(value)).build()).build())
                                      .collect(Collectors.toList())))
                              .build();
            futures[i / MAX_ITEMS_PER_BATCH] =
                getAsyncClient() == null ? CompletableFuture.runAsync(() -> getClient().batchWriteItem(request))
                                         : getAsyncClient().batchWriteItem(request);
        }
        return CompletableFuture.allOf(futures);
    }

    public CompletableFuture<ExtendedItemResult<T>> putItemExtendedAsync(T value) {
        return putItemAsync(PutItemRequest.builder()
                            .tableName(getTableName())
                            .item(encode(value))
                            .returnConsumedCapacity(ReturnConsumedCapacity.INDEXES)
                            .build())
                   .thenApply(resp -> new ExtendedItemResult<>(
                       resp.hasAttributes() ? decode(resp.attributes()) : null, resp.consumedCapacity()));
    }

    public CompletableFuture<ExtendedBatchResult<T, T>> putBatchExtendedAsync(List<T> values) {
        int numValues = values.size();
        var result = CompletableFuture.completedFuture(new ExtendedBatchResult<T, T>());
        for (int i = 0; i < numValues; i += MAX_ITEMS_PER_BATCH) {
            var request = BatchWriteItemRequest.builder()
                              .returnConsumedCapacity(ReturnConsumedCapacity.INDEXES)
                              .requestItems(Collections.singletonMap(getTableName(),
                                  values.subList(i, Math.min(numValues, i + MAX_ITEMS_PER_BATCH)).stream()
                                      .map(value -> WriteRequest.builder().putRequest(PutRequest.builder()
                                                                                          .item(encode(value)).build()).build())
                                      .collect(Collectors.toList())))
                              .build();
            result = result.thenCombine(
                getAsyncClient() == null ? CompletableFuture.supplyAsync(() -> getClient().batchWriteItem(request))
                                         : getAsyncClient().batchWriteItem(request),
                this::updateBatchResultForPut);
        }
        return result;
    }

    private PutItemResponse putItem(PutItemRequest request) {
        return getClient() == null ? getAsyncClient().putItem(request).join() : getClient().putItem(request);
    }

    private CompletableFuture<PutItemResponse> putItemAsync(PutItemRequest request) {
        return getAsyncClient() == null ? CompletableFuture.supplyAsync(() -> getClient().putItem(request)) : getAsyncClient().putItem(request);
    }

    private ExtendedBatchResult<T, T> updateBatchResultForPut(ExtendedBatchResult<T, T> result, BatchWriteItemResponse response) {
        if (response.hasUnprocessedItems()) {
            response.unprocessedItems().get(getTableName()).stream()
                .map(writeRequest -> decode(writeRequest.putRequest().item()))
                .forEach(item -> result.getUnprocessedValues().add(item));
        }
        if (response.hasConsumedCapacity()) {
            response.consumedCapacity().forEach(cap -> result.getCapacity().add(cap));
        }
        return result;
    }

    public final T deleteItem(T value) {
        return deleteItem(getPartitionKey(value), getSortKey(value));
    }

    public final T deleteItem(Key<PartitionT, SortT> key) {
        return deleteItem(key.getPartition(), key.getSort());
    }

    public T deleteItem(PartitionT partitionKey, SortT sortKey) {
        var deleteResponse = deleteItem(DeleteItemRequest.builder()
                   .tableName(getTableName())
                   .key(keysToMap(partitionKey, sortKey))
                   .build());
        return deleteResponse.hasAttributes() ? decode(deleteResponse.attributes()) : null;
    }

    /**
     * Get a batch of items. This can't be named "getBatch" because erasure makes it the same as the by-key version.
     * @param items The items whose keys will be used to get the batch
     */
    public void deleteBatchByItem(List<T> items) {
        deleteBatch(buildDeleteBatchesFromItems(items));
    }

    /**
     * Get a batch of items. This can't be named "getBatch" because erasure makes it the same as the by-item version.
     * @param keys The keys used to get the batch
     */
    public void deleteBatchByKey(List<Key<PartitionT, SortT>> keys) {
        deleteBatch(buildDeleteBatchesFromKeys(keys));
    }

    private void deleteBatch(List<Map<String, List<WriteRequest>>> batches) {
        for (var batch: batches) {
            var req = BatchWriteItemRequest.builder().requestItems(batch).build();
            if (getClient() == null) {
                getAsyncClient().batchWriteItem(req).join();
            } else {
                getClient().batchWriteItem(req);
            }
        }
    }

    public final ExtendedItemResult<T> deleteItemExtended(T value) {
        return deleteItemExtended(getPartitionKey(value), getSortKey(value));
    }

    public final ExtendedItemResult<T> deleteItemExtended(Key<PartitionT, SortT> value) {
        return deleteItemExtended(value.getPartition(), value.getSort());
    }

    public ExtendedItemResult<T> deleteItemExtended(PartitionT partitionKey, SortT sortKey) {
        var deleteResponse = deleteItem(DeleteItemRequest.builder()
                                        .tableName(getTableName())
                                        .key(keysToMap(partitionKey, sortKey))
                                        .returnConsumedCapacity(ReturnConsumedCapacity.INDEXES)
                                        .build());
        return new ExtendedItemResult<>(deleteResponse.hasAttributes() ? decode(deleteResponse.attributes()) : null, deleteResponse.consumedCapacity());
    }

    /**
     * Get a batch of items. This can't be named "getBatch" because erasure makes it the same as the by-key version.
     * @param items The items whose keys will be used to get the batch
     */
    public ExtendedBatchResult<T, Key<PartitionT, SortT>> deleteBatchByItemExtended(List<T> items) {
        return deleteBatchExtended(buildDeleteBatchesFromItems(items));
    }

    /**
     * Get a batch of items. This can't be named "getBatch" because erasure makes it the same as the by-item version.
     * @param keys The keys used to get the batch
     */
    public ExtendedBatchResult<T, Key<PartitionT, SortT>> deleteBatchByKeyExtended(List<Key<PartitionT, SortT>> keys) {
        return deleteBatchExtended(buildDeleteBatchesFromKeys(keys));
    }

    private ExtendedBatchResult<T, Key<PartitionT, SortT>> deleteBatchExtended(List<Map<String, List<WriteRequest>>> batches) {
        var result = new ExtendedBatchResult<T, Key<PartitionT, SortT>>();
        for (var batch: batches) {
            var req = BatchWriteItemRequest.builder().requestItems(batch).build();
            updateBatchResultForDelete(result, getClient() == null ? getAsyncClient().batchWriteItem(req).join() : getClient().batchWriteItem(req));
        }
        return result;
    }

    public final CompletableFuture<T> deleteItemAsync(T value) {
        return deleteItemAsync(getPartitionKey(value), getSortKey(value));
    }

    public final CompletableFuture<T> deleteItemAsync(Key<PartitionT, SortT> key) {
        return deleteItemAsync(key.getPartition(), key.getSort());
    }

    public CompletableFuture<T> deleteItemAsync(PartitionT partitionKey, SortT sortKey) {
        return deleteItemAsync(DeleteItemRequest.builder()
                               .tableName(getTableName())
                               .key(keysToMap(partitionKey, sortKey))
                               .build())
                   .thenApply(resp -> resp.hasAttributes() ? decode(resp.attributes()) : null);
    }

    /**
     * Get a batch of items. This can't be named "getBatch" because erasure makes it the same as the by-key version.
     * @param items The items whose keys will be used to get the batch
     */
    public CompletableFuture<Void> deleteBatchByItemAsync(List<T> items) {
        return deleteBatchAsync(buildDeleteBatchesFromItems(items));
    }

    /**
     * Get a batch of items. This can't be named "getBatch" because erasure makes it the same as the by-item version.
     * @param keys The keys used to get the batch
     */
    public CompletableFuture<Void> deleteBatchByKeyAsync(List<Key<PartitionT, SortT>> keys) {
        return deleteBatchAsync(buildDeleteBatchesFromKeys(keys));
    }

    private CompletableFuture<Void> deleteBatchAsync(List<Map<String, List<WriteRequest>>> batches) {
        CompletableFuture<?>[] futures = new CompletableFuture<?>[batches.size()];
        for (var i = 0; i < batches.size(); ++i) {
            var req = BatchWriteItemRequest.builder().requestItems(batches.get(i)).build();
            futures[i] = getAsyncClient() == null ? CompletableFuture.runAsync(() -> getClient().batchWriteItem(req)) : getAsyncClient().batchWriteItem(req);
        }
        return CompletableFuture.allOf(futures);
    }

    public final CompletableFuture<ExtendedItemResult<T>> deleteItemExtendedAsync(T value) {
        return deleteItemExtendedAsync(getPartitionKey(value), getSortKey(value));
    }

    public final CompletableFuture<ExtendedItemResult<T>> deleteItemExtendedAsync(Key<PartitionT, SortT> key) {
        return deleteItemExtendedAsync(key.getPartition(), key.getSort());
    }

    public CompletableFuture<ExtendedItemResult<T>> deleteItemExtendedAsync(PartitionT partitionKey, SortT sortKey) {
        return deleteItemAsync(DeleteItemRequest.builder()
                               .tableName(getTableName())
                               .key(keysToMap(partitionKey, sortKey))
                               .returnConsumedCapacity(ReturnConsumedCapacity.INDEXES)
                               .build())
                   .thenApply(resp -> new ExtendedItemResult<>(
                       resp.hasAttributes() ? decode(resp.attributes()) : null, resp.consumedCapacity()));
    }

    /**
     * Get a batch of items. This can't be named "getBatch" because erasure makes it the same as the by-key version.
     * @param items The items whose keys will be used to get the batch
     */
    public CompletableFuture<ExtendedBatchResult<T, Key<PartitionT, SortT>>> deleteBatchByItemExtendedAsync(List<T> items) {
        return deleteBatchExtendedAsync(buildDeleteBatchesFromItems(items));
    }

    /**
     * Get a batch of items. This can't be named "getBatch" because erasure makes it the same as the by-item version.
     * @param keys The keys used to get the batch
     */
    public CompletableFuture<ExtendedBatchResult<T, Key<PartitionT, SortT>>> deleteBatchByKeyExtendedAsync(List<Key<PartitionT, SortT>> keys) {
        return deleteBatchExtendedAsync(buildDeleteBatchesFromKeys(keys));
    }

    private CompletableFuture<ExtendedBatchResult<T, Key<PartitionT, SortT>>> deleteBatchExtendedAsync(List<Map<String, List<WriteRequest>>> batches) {
        var futureResult = CompletableFuture.completedFuture(new ExtendedBatchResult<T, Key<PartitionT, SortT>>());
        for (var batch : batches) {
            var req = BatchWriteItemRequest.builder().requestItems(batch).build();
            futureResult = futureResult.thenCombine(
                getAsyncClient() == null ? CompletableFuture.supplyAsync(() -> getClient().batchWriteItem(req))
                                         : getAsyncClient().batchWriteItem(req),
                this::updateBatchResultForDelete);
        }
        return futureResult;
    }

    private ExtendedBatchResult<T, Key<PartitionT, SortT>> updateBatchResultForDelete(ExtendedBatchResult<T, Key<PartitionT, SortT>> result, BatchWriteItemResponse response) {
        if (response.hasUnprocessedItems()) {
            response.unprocessedItems().get(getTableName()).stream()
                .map(writeRequest -> writeRequest.deleteRequest().key())
                .map(m -> new Key<>(getPartitionKey(m.get(getPartitionKeyAttribute())), getSortKey(m.get(getSortKeyAttribute()))))
                .forEach(k -> result.getUnprocessedValues().add(k));
        }
        if (response.hasConsumedCapacity()) {
            response.consumedCapacity().forEach(cap -> result.getCapacity().add(cap));
        }
        return result;
    }

    private DeleteItemResponse deleteItem(DeleteItemRequest request) {
        return getClient() == null ? getAsyncClient().deleteItem(request).join() : getClient().deleteItem(request);
    }

    private CompletableFuture<DeleteItemResponse> deleteItemAsync(DeleteItemRequest request) {
        return getAsyncClient() == null ? CompletableFuture.supplyAsync(() -> getClient().deleteItem(request)) : getAsyncClient().deleteItem(request);
    }

    public abstract <SecondaryPartitionT, SecondarySortT> DynamoIndex<T, SecondaryPartitionT, SecondarySortT> getIndex(
        String name, Class<SecondaryPartitionT> secondaryPartitionClass, Class<SecondarySortT> secondarySortClass);

    /**
     * Build a list of maps from table name to a list of keys. Each map will have at most MAX_ITEMS_PER_BATCH items.
     * @param items A list of items to use as sort keys
     * @return A list of batches to submit for processing
     */
    private List<Map<String, List<WriteRequest>>> buildDeleteBatchesFromItems(List<T> items) {
        int numItems = items.size();
        var result = new ArrayList<Map<String, List<WriteRequest>>>((numItems + MAX_ITEMS_PER_BATCH - 1) / MAX_ITEMS_PER_BATCH);
        for (int offset = 0; offset < numItems; offset += MAX_ITEMS_PER_BATCH) {
            result.add(Collections.singletonMap(getTableName(),
                items.subList(offset, Math.min(numItems, offset + MAX_ITEMS_PER_BATCH)).stream()
                    .map(item -> WriteRequest.builder().deleteRequest(DeleteRequest.builder().key(keysToMap(getPartitionKey(item), getSortKey(item))).build()).build())
                    .collect(Collectors.toList())));
        }
        return result;
    }

    /**
     * Build a list of maps from table name to a list of keys. Each map will have at most MAX_ITEMS_PER_BATCH items.
     * @param keys A list of keys
     * @return A list of batches to submit for processing
     */
    private List<Map<String, List<WriteRequest>>> buildDeleteBatchesFromKeys(List<Key<PartitionT, SortT>> keys) {
        int numItems = keys.size();
        var result = new ArrayList<Map<String, List<WriteRequest>>>((numItems + MAX_ITEMS_PER_BATCH - 1) / MAX_ITEMS_PER_BATCH);
        for (int offset = 0; offset < numItems; offset += MAX_ITEMS_PER_BATCH) {
            result.add(Collections.singletonMap(getTableName(),
                keys.subList(offset, Math.min(numItems, offset + MAX_ITEMS_PER_BATCH)).stream()
                    .map(key -> WriteRequest.builder().deleteRequest(DeleteRequest.builder().key(keysToMap(key.getPartition(), key.getSort())).build()).build())
                    .collect(Collectors.toList())));
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
