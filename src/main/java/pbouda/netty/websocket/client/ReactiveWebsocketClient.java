package pbouda.netty.websocket.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.WebSocketClientProtocolConfig;
import io.netty.handler.codec.http.websocketx.WebSocketClientProtocolHandler;
import io.netty.handler.codec.http.websocketx.WebSocketVersion;
import io.netty.handler.ssl.SslContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static net.logstash.logback.argument.StructuredArguments.kv;

public class ReactiveWebsocketClient {

    private static final Logger LOG = LoggerFactory.getLogger(ReactiveWebsocketClient.class);

    private volatile Channel channel;

    private final Bootstrap bootstrap;
    private final URI uri;
    private final EventHandler eventHandler;
    private final Duration websocketHandshakeTimeout;

    public ReactiveWebsocketClient(SslContext sslContext, NotificationsManager notificationsManager, URI uri) {
        this.websocketHandshakeTimeout = Duration.ofSeconds(10);

        WebSocketClientProtocolConfig websocketConfig = WebSocketClientProtocolConfig.newBuilder()
                .handshakeTimeoutMillis(websocketHandshakeTimeout.toMillis())
                .webSocketUri(uri)
                .handleCloseFrames(true)
                .version(WebSocketVersion.V13)
                .build();

        this.uri = uri;
        this.eventHandler = new EventHandler();

        WebsocketClientInitializer initializer = new WebsocketClientInitializer(
                sslContext, websocketConfig, eventHandler, new NotificationHandler(notificationsManager));

        this.bootstrap = new Bootstrap()
                .group(new NioEventLoopGroup())
                .channel(NioSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .handler(initializer);
    }

    /**
     * Connects the {@link ReactiveWebsocketClient} to a websocket server and register callbacks for events:
     */
    public void connect() {
        /*
         * Connect to a websocket server and waits for a result:
         * - a blocking operation
         * - register a notification of successful connection
         * - register a notification when the server starts stopping
         * - cache current Channel for sending messages
         */
        ChannelFuture channelFuture = bootstrap.connect(uri.getHost(), uri.getPort())
                .syncUninterruptibly()
                .addListener(f -> LOG.info("WEBSOCKET_CLIENT: Client connected: " + uri))
                .channel().closeFuture()
                .addListener(f -> LOG.warn("WEBSOCKET_CLIENT: Closing a client", f.cause()));
        this.channel = channelFuture.channel();

        /*
         * Synchronization point between an application and prepared websocket client.
         * At this point the websocket client is supposed to be fully prepared:
         * - SSL Handshake
         * - WebSocket Handshake
         * - All handlers activated
         */
        eventHandler.waitForActivation(websocketHandshakeTimeout);

        /*
         * Sending HeartBeat messages every 10 seconds:
         * - single instance of HearBeatCollector for all heartbeats
         * - gathers information about heartbeats and takes actions
         */
        HeartBeatCollector consumer = new HeartBeatCollector();
        channelFuture.channel().eventLoop()
                .scheduleAtFixedRate(() -> sendHeartBeat(consumer), 10, 10, TimeUnit.SECONDS);
    }

    /**
     * Sends a heartbeat message to the connected socket and asynchronously waits
     * for the response. There is {@code consumer} that consumes the
     * response and calculates whether a threshold has been passed.
     *
     * @param consumer a shared consumer that collects responses from all heartbeats
     */
    private void sendHeartBeat(HeartBeatCollector consumer) {
        String correlationId = UUID.randomUUID().toString();
        ClientHeartbeatRequest request = ClientHeartbeatRequest.newBuilder()
                .setCorrelationId(correlationId)
                .setSentDate(DateUtils.now())
                .build();

        submit(correlationId, request.toString(), "HeartBeat", consumer);
    }

    /**
     * Creates a new {@link OutboundMessage} and sends it to Netty's outbound
     * channel to send it out via a websocket.
     * Moreover, it creates {@link ResponsePublisher} that waits for a response
     * and propagates it via {@link Mono} that is returned to chain other
     * processing method.
     *
     * @param correlationId message's correlation ID.
     * @param message       message to be sent out via websocket
     * @param tag           tag of the current processing.
     * @return {@link Mono} to proceed with a processing of a response message.
     */
    public Mono<InboundMessage> submit(String correlationId, String message, String tag) {
        ResponsePublisher publisher = new ResponsePublisher(correlationId, tag);
        channel.writeAndFlush(new OutboundMessage(correlationId, message, publisher));
        return Mono.from(publisher);
    }

    /**
     * Creates a new {@link OutboundMessage} and sends it to Netty's outbound
     * channel to send it out via websocket.
     * Moreover, it accepts {@link Consumer} that is registered as a subscriber
     * for the response from a websocket to consumes when it's propagated from
     * downstream processing.
     *
     * @param correlationId message's correlation ID.
     * @param message       message to be sent out via websocket
     * @param tag           tag of the current processing.
     * @param callback      callback that consumes a response.
     */
    public void submit(
            String correlationId, String message, String tag, Consumer<InboundMessage> callback) {

        ResponsePublisher publisher = new ResponsePublisher(correlationId, tag);
        channel.writeAndFlush(new OutboundMessage(correlationId, message, publisher));
        Mono.from(publisher).subscribe(callback);
    }

    /**
     * Consumes responses from heartbeat calls and calculates the threshold whether
     * to take any action (e.g. reconnect).
     * <p>
     * This class is not thread-safe, it's going to be used in Netty that means that
     * it runs always on the same thread.
     */
    private static class HeartBeatCollector implements Consumer<InboundMessage> {

        private static final Logger LOG = LoggerFactory.getLogger(HeartBeatCollector.class);

        private int failedAttempts = 0;

        @Override
        public void accept(InboundMessage message) {
            if (message.getType() == InboundMessage.Type.SUCCESS) {
                failedAttempts = 0;
                LOG.info("WEBSOCKET_CLIENT: Successful heartbeat, {}",
                        kv("correlation-id", message.getCorrelationId()));
            } else {
                failedAttempts++;
                LOG.error("WEBSOCKET_CLIENT: Heartbeat failed, {} {}",
                        kv("attempt", failedAttempts),
                        kv("correlation-id", message.getCorrelationId()));
            }
        }
    }

    /**
     * An initializer builds Netty's pipeline.
     */
    private static class WebsocketClientInitializer extends ChannelInitializer<SocketChannel> {
        private final SslContext sslContext;
        private final WebSocketClientProtocolConfig websocketConfig;
        private final EventHandler eventHandler;
        private final NotificationHandler notificationHandler;

        public WebsocketClientInitializer(
                SslContext sslContext,
                WebSocketClientProtocolConfig websocketConfig,
                EventHandler eventHandler,
                NotificationHandler notificationHandler) {

            this.sslContext = sslContext;
            this.websocketConfig = websocketConfig;
            this.eventHandler = eventHandler;
            this.notificationHandler = notificationHandler;
        }

        @Override
        public void initChannel(SocketChannel ch) {
            ChannelPipeline pipeline = ch.pipeline();
            // pipeline.addLast(new LoggingHandler(LogLevel.TRACE));
            pipeline.addLast(sslContext.newHandler(ch.alloc()));
            pipeline.addLast(new HttpClientCodec());
            pipeline.addLast(new HttpObjectAggregator(8192));
            // pipeline.addLast(WebSocketClientCompressionHandler.INSTANCE);
            pipeline.addLast(new WebSocketClientProtocolHandler(websocketConfig));
            pipeline.addLast(new WebSocketOutboundEncoder());
            pipeline.addLast(new ResponseHandler());
            pipeline.addLast(notificationHandler);
            pipeline.addLast(eventHandler);
        }
    }
}
