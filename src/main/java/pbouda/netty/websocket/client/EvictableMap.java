package pbouda.netty.websocket.client;

import java.time.Duration;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * {@link EvictableMap} is intentionally not Thread-Safe
 * because of nature of {@link io.netty.channel.EventLoop} that
 * ensures that one {@link io.netty.channel.Channel} is always
 * served by a single thread.
 * <p>
 * In case of any need to ensure multi-threaded access, we
 * need to synchronize accessing methods to {@link #map}.
 * <p>
 * {@link #start} needs to be called to start eviction mechanism and
 * accepts the same {@link java.util.concurrent.ExecutorService}
 * that is used to access methods {@link #put(String, Evictable)}
 * and {@link #getAndRemove(String)}.
 */
public class EvictableMap<T extends EvictableMap.Evictable> implements AutoCloseable {

    /**
     * An interface that need to be implemented by every items
     * that wants to be handled by {@link EvictableMap}.
     */
    public interface Evictable {

        /**
         * Returns a timestamp in millis that is taken into
         * account during evicting process.
         *
         * If timestamp is already in past then the item will be
         * evicted immediately in the next iteration.
         *
         * @return time for an eviction mechanism.
         */
        long getTimestamp();

        /**
         * A callback that is called on the item when the item
         * is being evicted from the map.
         *
         * @param timestamp timestamp of the measurement, e.g. to calculate
         *                  a total duration.
         */
        void publishTimeout(long timestamp);
    }

    /**
     * A map containing all items managed by this component. The items
     * are checked on a regular time-base determined by {@code evictionPeriod}
     * using {@link #evictingTask}.
     */
    private final LinkedHashMap<String, T> map = new LinkedHashMap<>();

    /**
     * Determines an interval for checking the state of tasks.
     */
    private final long evictionPeriodInNanos;

    /**
     * Determines how long a task is going to be kept in the map.
     * <p>
     * The timeout is not precise because it is determined by a granularity of
     * {@link #evictionPeriodInNanos}.
     */
    private final long taskTimeoutInNanos;

    /**
     * A recurring task that removes all timed out tasks
     * based on {@link Evictable} interface.
     */
    private ScheduledFuture<?> evictingTask;

    public EvictableMap(Duration evictionPeriod, Duration taskTimeout) {
        this.evictionPeriodInNanos = evictionPeriod.toNanos();
        this.taskTimeoutInNanos = taskTimeout.toNanos();
    }

    public void put(String correlationId, T publisher) {
        map.put(correlationId, publisher);
    }

    public T getAndRemove(String correlationId) {
        return map.remove(correlationId);
    }

    public void start(ScheduledExecutorService ticker) {
        this.evictingTask = ticker.scheduleAtFixedRate(this::eviction, 0,
            this.evictionPeriodInNanos, TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() {
        this.evictingTask.cancel(false);
    }

    /**
     * Removes and notifies all objects that implements {@link Evictable}
     * interface and the elapsed time from {@link Evictable#getTimestamp()}
     * is longer than {@link #taskTimeoutInNanos}.
     */
    private void eviction() {
        long measurementStart = System.nanoTime();
        long threshold = measurementStart - taskTimeoutInNanos;

        Iterator<Map.Entry<String, T>> iterator = map.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, T> entry = iterator.next();
            T evictable = entry.getValue();
            long timestamp = evictable.getTimestamp();
            if (timestamp > threshold) {
                iterator.remove();
                evictable.publishTimeout(measurementStart);
            } else {
                break;
            }
        }
    }
}
