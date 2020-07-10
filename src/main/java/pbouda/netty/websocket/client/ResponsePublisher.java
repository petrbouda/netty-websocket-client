package pbouda.netty.websocket.client;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.time.Duration;

import static net.logstash.logback.argument.StructuredArguments.kv;

public class ResponsePublisher implements Publisher<InboundMessage>, EvictableMap.Evictable {

    private static final Logger LOG = LoggerFactory.getLogger(ResponsePublisher.class);

    private final String tag;
    private final String correlationId;
    private final CachingSubscription subscription;
    private final long processingTimestamp;
    private long ioTimestamp;

    /**
     * Creates a new {@link ResponsePublisher} representing an expected and
     * incoming response.
     *
     * @param correlationId ID of the expected message.
     * @param tag           Tag of the current processing.
     */
    public ResponsePublisher(String correlationId, String tag) {
        this.processingTimestamp = System.nanoTime();
        this.subscription = new CachingSubscription();
        this.correlationId = correlationId;
        this.tag = tag;
    }

    public void startIO() {
        this.ioTimestamp = System.nanoTime();
    }

    @Override
    public void subscribe(Subscriber<? super InboundMessage> subscriber) {
        subscription.setRealSubscriber(subscriber);
        subscriber.onSubscribe(subscription);
    }

    public void publish(InboundMessage message) {
        message.setProcessingTimestamp(processingTimestamp);
        message.setTag(tag);
        subscription.signal(message);
    }

    @Override
    public void publishTimeout(long measurementTimestamp) {
        Duration elapsed = Duration.ofNanos(measurementTimestamp - ioTimestamp);

        InboundMessage message = InboundMessage.timeout(correlationId);
        message.setProcessingTimestamp(processingTimestamp);
        message.setTag(tag);
        subscription.signal(message);

        LOG.warn("WEBSOCKET_CLIENT: publish timeout notification {} {} {}",
            kv("duration", elapsed.toMillis()),
            kv("tag", tag),
            kv("correlation-id", correlationId));
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public String getTag() {
        return tag;
    }

    @Override
    public long getTimestamp() {
        return ioTimestamp;
    }

    public long getProcessingTimestamp() {
        return processingTimestamp;
    }

    private static class CachingSubscription implements Subscription {

        /**
         * Just a marker that signals that netty's thread has already
         * an instance ready and it's cached.
         */
        private static final Subscriber<?> MARKER = new Subscriber<>() {
            @Override
            public void onSubscribe(Subscription s) {
            }

            @Override
            public void onNext(Object o) {
            }

            @Override
            public void onError(Throwable t) {
            }

            @Override
            public void onComplete() {
            }
        };

        /**
         * Not sure if {@link Publisher#subscribe(Subscriber)}
         * is called from the same thread as {@link Subscription#request(long)}
         * <p>
         * - sharing subscriber with netty's thread needs to be delayed after
         * {@link Subscription#request(long)}.
         */
        private volatile Subscriber<InboundMessage> realSubscriber;

        /**
         * Netty thread is faster then subscriber, cache the value for him.
         */
        private volatile InboundMessage inboundMessage;

        private volatile Subscriber subscriber;
        private static final VarHandle SUBSCRIBER;

        private volatile boolean terminated;
        private static final VarHandle TERMINATED;

        static {
            try {
                MethodHandles.Lookup l = MethodHandles.lookup();
                SUBSCRIBER = l.findVarHandle(CachingSubscription.class, "subscriber", Subscriber.class);
                TERMINATED = l.findVarHandle(CachingSubscription.class, "terminated", boolean.class);
            } catch (ReflectiveOperationException e) {
                throw new Error(e);
            }
        }

        @Override
        public void request(long n) {
            if (!SUBSCRIBER.compareAndSet(this, null, realSubscriber)) {
                while (inboundMessage == null) {
                    Thread.onSpinWait();
                }

                sendSignal(realSubscriber, inboundMessage);
            }
        }

        @Override
        public void cancel() {
            TERMINATED.set(this, true);
        }

        public void signal(InboundMessage message) {
            if (SUBSCRIBER.compareAndSet(this, null, MARKER)) {
                this.inboundMessage = message;
            } else {
                @SuppressWarnings("unchecked")
                Subscriber<InboundMessage> subscriber = (Subscriber<InboundMessage>) SUBSCRIBER.get(this);
                sendSignal(subscriber, message);
            }
        }

        @SuppressWarnings("unchecked")
        public void setRealSubscriber(Subscriber realSubscriber) {
            this.realSubscriber = realSubscriber;
        }

        private void sendSignal(Subscriber<InboundMessage> subscriber, InboundMessage message) {
            if (TERMINATED.compareAndSet(this, false, true)) {
                if (message.getType() == InboundMessage.Type.SUCCESS) {
                    subscriber.onNext(message);
                    subscriber.onComplete();
                } else if (message.getType() == InboundMessage.Type.ERROR) {
                    subscriber.onError(new WsException.Error(message));
                } else if (message.getType() == InboundMessage.Type.TIMEOUT) {
                    subscriber.onError(new WsException.Timeout(message));
                }
            }
        }
    }
}
