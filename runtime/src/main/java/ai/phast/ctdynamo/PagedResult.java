package ai.phast.ctdynamo;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConsumedCapacity;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * This extends IterableResult to handle the paging. It is separated mostly to hide the ResponseT parameter from the
 * application.
 *
 * @param <T> The type of item returned by the scan
 * @param <ResponseT> The type of dynamo response that this PaginatedResult processes. This is not relevant to the
 *                   application.
 */
abstract class PagedResult<T, ResponseT> extends IterableResult<T> {

    private CompletableFuture<ResponseT> futureResponse;

    private Iterator<Map<String, AttributeValue>> responseIterator = null;

    private Map<String, AttributeValue> exclusiveStart;

    PagedResult(DynamoIndex<T, ?, ?> index, int limit) {
        super(index, limit);
    }

    /** This needs to be called after the constructor is done. It starts the fetch of the first page */
    void init() {
        if (getLimit() != 0) {
            // Limit 0 is a special case. We never bother to do a request then.
            futureResponse = fetchNextPage(null);
        }
    }

    abstract CompletableFuture<ResponseT> fetchNextPage(Map<String, AttributeValue> exclusiveStart);

    private boolean iteratorHasNext() {
        while (true) {
            if ((responseIterator != null) && responseIterator.hasNext()) {
                // We have an iterator, it has another element
                return true;
            }
            responseIterator = null; // Indicate we do not have a useful iterator
            if (futureResponse == null) {
                // We have nothing from a current itorator and no operation in progress
                return false;
            }

            var response = futureResponse.join();

            // Update counters with data from the new request
            addNumItemsFound(getCount(response));
            addNumItemsScanned(getScannedCount(response));
            getCapacity().add(getRawCapacity(response));

            futureResponse = null;
            var nextQueryStart = getLastEvaluatedKey(response);
            var lastItemSeen = nextQueryStart;
            var list = getItems(response);
            var listSize = list.size();
            if ((getLimit() >= 0) && (listSize + getNumItemsReturned() >= getLimit())) {
                // This page completes the operation by reaching (or exceeding) our limit
                nextQueryStart = null; // Don't ask for another page
                if (listSize + getNumItemsReturned() > getLimit()) {
                    // This page has too many items, it would exceed our limit. Chop off the tail of our list.
                    listSize = getLimit() - getNumItemsReturned();
                    lastItemSeen = list.get(listSize - 1);
                    list = list.subList(0, listSize);
                }
            }
            addNumItemsReturned(listSize);
            responseIterator = list.iterator();
            if (nextQueryStart == null) {
                // Not asking for another page. Record the last item seen (if it exists) as the next query start.
                exclusiveStart = lastItemSeen;
            } else {
                // Ask for another page
                fetchNextPage(nextQueryStart);
            }
        }
    }

    private T iteratorNext() {
        return getIndex().decode(responseIterator.next());
    }

    abstract int getScannedCount(ResponseT response);

    abstract int getCount(ResponseT response);

    abstract ConsumedCapacity getRawCapacity(ResponseT response);

    abstract Map<String, AttributeValue> getLastEvaluatedKey(ResponseT response);

    abstract List<Map<String, AttributeValue>> getItems(ResponseT response);

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

    /**
     * Get an exclusive start key that will continue where this iterator leaves off. This will be null if there are
     * no more items in the query or scane (which will always be the case when you run a query or scan with no limit).
     * <p>You must first get to the end of the iterator or stream, then call this function; if you want to continue from
     * an earlier point, then use the last item you get as your exclusive start, via {@link DynamoIndex#getExclusiveStart(Object)}.
     * @return The key that lets you resume this operation where it left off
     * @throws IllegalStateException If this is called before the end of the iterator or stream has been reached
     */
    public Map<String, AttributeValue> getExclusiveStart() {
        if ((futureResponse != null) || (responseIterator != null)) {
            // Not allowed to ask for the exclusive start until we have reached the end
            throw new IllegalStateException("The exclusive start is unknown until the iterator or stream reaches the end");
        }
        return exclusiveStart;
    }

    public Stream<T> stream() {
        return StreamSupport.stream(spliterator(), false);
    }
}
