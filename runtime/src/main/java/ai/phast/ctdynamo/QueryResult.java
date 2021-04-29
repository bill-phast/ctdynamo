package ai.phast.ctdynamo;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class QueryResult<T> implements Iterable<T>, Iterator<T> {

    private final QueryRequest.Builder queryBuilder;

    private CompletableFuture<QueryResponse> futureResponse;

    private final DynamoIndex<T, ?, ?> index;

    private final Integer limit;

    private int totalConsumed = 0;

    private boolean done = false;

    private Iterator<Map<String, AttributeValue>> responseIterator = null;

    private Map<String, AttributeValue> exclusiveStart;

    QueryResult(DynamoIndex<T, ?, ?> index, QueryRequest.Builder queryBuilder, Integer limit) {
        this.queryBuilder = queryBuilder;
        this.index = index;
        this.limit = limit;
        doRequest();
    }

    private void doRequest() {
        if (futureResponse != null) {
            throw new RuntimeException("Got doRequest while we have a pending request");
        }
        var request = queryBuilder.build();
        futureResponse = (index.getAsyncClient() == null ? CompletableFuture.supplyAsync(() -> index.getClient().query(request)) : index.getAsyncClient().query(request));
    }

    @Override
    public boolean hasNext() {
        while (true) {
            if (responseIterator != null) {
                if (responseIterator.hasNext()) {
                    // We have an iterator, it has another element, thus we have another.
                    return true;
                }
                if (done) {
                    // We have an iterator, it has no more elements, we are done. Thus we have no more elements.
                    return false;
                }
                // We have an iterator, it has no more elements, but we are not done! Wait for the next task to
                // finish.
            }
            // Now we have released our lock so we can join with our worker thread.
            var response = futureResponse.join();
            futureResponse = null;
            var list = (response.hasItems() ? response.items()
                                            : Collections.<Map<String, AttributeValue>>emptyList());
            int listSize = list.size();
            if (response.lastEvaluatedKey() == null) {
                // There are no responses after this one. We are done once we process this one.
                done = true;
            }
            if (limit != null && listSize + totalConsumed >= limit) {
                // Once we process this, we will have read our limit.
                done = true;
                if (listSize + totalConsumed > limit) {
                    listSize = limit - totalConsumed;
                    list = list.subList(0, listSize);
                    exclusiveStart = list.get(listSize - 1);
                } else {
                    exclusiveStart = response.lastEvaluatedKey();
                }
            }
            totalConsumed += listSize;
            responseIterator = list.iterator();
            if (!done) {
                // We aren't done, so let's start another query from where we left off.
                queryBuilder.exclusiveStartKey(response.lastEvaluatedKey());
                doRequest();
            }
        }
    }

    @Override
    public synchronized T next() {
        return index.decode(responseIterator.next());
    }

    @Override
    public Iterator<T> iterator() {
        return this;
    }

    private Map<String, AttributeValue> getExclusiveStart() {
        if (!done) {
            throw new IllegalStateException();
        }
        return exclusiveStart;
    }

    public Stream<T> stream() {
        return StreamSupport.stream(spliterator(), false);
    }
}
