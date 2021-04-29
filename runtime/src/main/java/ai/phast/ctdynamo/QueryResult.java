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

public class QueryResult<T> implements Iterable<T> {

    private final QueryRequest.Builder queryBuilder;

    private CompletableFuture<QueryResponse> futureResponse;

    private final DynamoIndex<T, ?, ?> index;

    private final int limit;

    private int totalConsumed = 0;

    private Iterator<Map<String, AttributeValue>> responseIterator = null;

    private Map<String, AttributeValue> exclusiveStart;

    QueryResult(DynamoIndex<T, ?, ?> index, QueryRequest.Builder queryBuilder, int limit) {
        this.queryBuilder = queryBuilder;
        this.index = index;
        this.limit = limit;
        if (limit != 0) {
            // Limit 0 is a special case. We never bother to do a request then.
            doRequest();
        }
    }

    private void doRequest() {
        var request = queryBuilder.build();
        futureResponse = (index.getAsyncClient() == null ? CompletableFuture.supplyAsync(() -> index.getClient().query(request)) : index.getAsyncClient().query(request));
    }

    private boolean iteratorHasNext() {
        while (true) {
            if ((responseIterator != null) && responseIterator.hasNext()) {
                // We have an iterator, it has another element
                return true;
            }
            responseIterator = null; // Indicate we do not have a useful iterator
            if (futureResponse == null) {
                // We have nothing from a current itorator and nothing coming.
                return false;
            }

            // Now we have released our lock so we can join with our worker thread.
            var response = futureResponse.join();
            futureResponse = null;
            var nextQueryStart = response.lastEvaluatedKey();
            var lastItemSeen = nextQueryStart;
            if (response.hasItems()) {
                var list = response.items();
                var listSize = list.size();
                if ((limit >= 0) && (listSize + totalConsumed >= limit)) {
                    nextQueryStart = null; // Don't ask for another page
                    if (listSize + totalConsumed > limit) {
                        // Oops, we got too much. Chop off the tail of our list.
                        listSize = limit - totalConsumed;
                        // Chop the list down to the size we want. Save the last item from this shorter list.
                        lastItemSeen = list.get(listSize - 1);
                        list = list.subList(0, listSize);
                    }
                }
                totalConsumed += listSize;
                responseIterator = list.iterator();
            }
            if (nextQueryStart == null) {
                // Not asking for another page. Record the last item seen (if it exists) as the next query start.
                exclusiveStart = lastItemSeen;
            } else {
                // Ask for another page
                queryBuilder.exclusiveStartKey(nextQueryStart);
                doRequest();
            }
        }
    }

    private T iteratorNext() {
        return index.decode(responseIterator.next());
    }

    @Override
    public Iterator<T> iterator() {
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                return iteratorHasNext();
            }

            @Override
            public T next() {
                return iteratorNext();
            }
        };
    }

    private Map<String, AttributeValue> getExclusiveStart() {
        if ((futureResponse != null) || (responseIterator != null)) {
            // Not allowed to ask for the exclusive start until we have reached the end
            throw new IllegalStateException();
        }
        return exclusiveStart;
    }

    public Stream<T> stream() {
        return StreamSupport.stream(spliterator(), false);
    }
}
