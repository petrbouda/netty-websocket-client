package pbouda.netty.websocket.client;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

import static net.logstash.logback.argument.StructuredArguments.kv;

public class ResponseHandler extends ChannelDuplexHandler {
    private static final Logger LOG = LoggerFactory.getLogger(ResponseHandler.class);

    private final EvictableMap<ResponsePublisher> publishers;

    public ResponseHandler() {
        this(Duration.ofSeconds(1), Duration.ofSeconds(6));
    }

    public ResponseHandler(Duration evictionPeriod, Duration taskTimeout) {
        this.publishers = new EvictableMap<>(evictionPeriod, taskTimeout);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        ctx.fireChannelActive();
        this.publishers.start(ctx.executor());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        ctx.fireChannelInactive();
        this.publishers.close();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object obj) {
        if (obj instanceof TextWebSocketFrame) {
            channelRead0(ctx, (TextWebSocketFrame) obj);
        } else {
            LOG.warn("WEBSOCKET_CLIENT: Unknown message type: " + obj.getClass());
            ctx.fireChannelRead(obj);
        }
    }

    private void channelRead0(ChannelHandlerContext ctx, TextWebSocketFrame frame) {
        InboundMessage message = InboundMessage.parse(frame.text());

        if (message.getType() == InboundMessage.Type.SUCCESS
            || message.getType() == InboundMessage.Type.ERROR) {

            ResponsePublisher publisher = publishers.getAndRemove(message.getCorrelationId());
            Duration duration = Duration.ofNanos(System.nanoTime() - publisher.getTimestamp());
            LOG.info("WEBSOCKET_CLIENT: Received {} {} {} {}",
                kv("result", message.getType()),
                kv("tag", publisher.getTag()),
                kv("correlation-id", publisher.getCorrelationId()),
                kv("duration", duration.toMillis()));

            publisher.publish(message);
        } else if (message.getType() == InboundMessage.Type.NOTIFICATION) {
            ctx.fireChannelRead(message);
        }
    }

    @Override
    public void write(ChannelHandlerContext context, Object obj, ChannelPromise promise) {
        if (obj instanceof OutboundMessage) {
            OutboundMessage outbound = (OutboundMessage) obj;
            ResponsePublisher publisher = outbound.getPublisher();
            publishers.put(outbound.getCorrelationId(), publisher);
            // Start measuring time
            publisher.startIO();
        }
        context.writeAndFlush(obj, promise);
    }
}
