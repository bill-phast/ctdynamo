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
 * A paginated result, from a scan or a query. Dynamo returns pages of results; we start fetching page n+1 as soon as
 * the application starts reading page n. If the application reads to the start of a page before the page is ready,
 * then it will wait until the page becomes available.
 *
 * <p>Applications should use the iterator() or stream() calls to return the
 * data.
 * @param <T> The type of item returned by the scan
 * @param <ResponseT> The type of dynamo response that this PaginatedResult processes. This is not relevant to the
 *                   application.
 */
public abstract class IterableResult<T, ResponseT> implements Iterable<T> {

    private CompletableFuture<ResponseT> futureResponse;

    private final DynamoIndex<T, ?, ?> index;

    private final int limit;

    /** Count of all items returned by all queries. Includes the items that we threw away if the last request had too many */
    private int itemsFound;

    /** Count of all items scanned by all queries */
    private int itemsScanned;

    /** Count of all items returned to the iterator */
    private int itemsReturned;

    private Iterator<Map<String, AttributeValue>> responseIterator = null;

    private Map<String, AttributeValue> exclusiveStart;

    private final CapacityUsed capacity = new CapacityUsed();

    IterableResult(DynamoIndex<T, ?, ?> index, int limit) {
        this.index = index;
        this.limit = limit;
    }

    final DynamoIndex<T, ?, ?> getIndex() {
        return index;
    }

    /** This needs to be called after the constructor is done. It starts the fetch of the first page */
    void init() {
        if (limit != 0) {
            // Limit 0 is a special case. We never bother to do a request then.
            futureResponse = fetchNextPage(null);
        }
    }

    protected abstract CompletableFuture<ResponseT> fetchNextPage(Map<String, AttributeValue> exclusiveStart);

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
            itemsFound += getCount(response);
            itemsScanned += getScannedCount(response);
            capacity.add(getRawCapacity(response));

            futureResponse = null;
            var nextQueryStart = getLastEvaluatedKey(response);
            var lastItemSeen = nextQueryStart;
            var list = getItems(response);
            var listSize = list.size();
            if ((limit >= 0) && (listSize + itemsReturned >= limit)) {
                // This page completes the operation by reaching (or exceeding) our limit
                nextQueryStart = null; // Don't ask for another page
                if (listSize + itemsReturned > limit) {
                    // This page has too many items, it would exceed our limit. Chop off the tail of our list.
                    listSize = limit - itemsReturned;
                    lastItemSeen = list.get(listSize - 1);
                    list = list.subList(0, listSize);
                }
            }
            itemsReturned += listSize;
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

    protected abstract int getScannedCount(ResponseT response);

    protected abstract int getCount(ResponseT response);

    protected abstract ConsumedCapacity getRawCapacity(ResponseT response);

    protected abstract Map<String, AttributeValue> getLastEvaluatedKey(ResponseT response);

    protected abstract List<Map<String, AttributeValue>> getItems(ResponseT response);

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

    /**
     * Return the total number of items returned so far by this query or scan
     * @return The total number of items return so far by this query or scan
     */
    public int getNumItemsReturned() {
        return itemsReturned;
    }

    /**
     * Return the total number of items that have been scanned so far by this query or scan. When there is no query
     * filter, this will always be equal to getItemsReturned.
     * @return The total number of items that have been scanned so far by this query or scan.
     */
    public int getNumItemsScanned() {
        return itemsScanned;
    }

    /**
     * Return the total number of items that have been found by this query or scan. This is equal to getItemsReturned()
     * in most cases; it will be more if the last page of results exceeded the limit specified.
     * @return The total number of items that have been found by this query or scan
     */
    public int getNumItemsFound() {
        return itemsFound;
    }

    public CapacityUsed getCapacity() {
        return capacity;
    }

    public Stream<T> stream() {
        return StreamSupport.stream(spliterator(), false);
    }
}
