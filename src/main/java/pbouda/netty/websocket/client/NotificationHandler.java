package pbouda.netty.websocket.client;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static net.logstash.logback.argument.StructuredArguments.kv;

@ChannelHandler.Sharable
public class NotificationHandler extends SimpleChannelInboundHandler<InboundMessage> {

    private static final Logger LOG = LoggerFactory.getLogger(NotificationHandler.class);

    private final NotificationsManager notificationsManager;

    public NotificationHandler(NotificationsManager notificationsManager) {
        super(InboundMessage.class);
        this.notificationsManager = notificationsManager;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, InboundMessage message) {
        if (message.getType() == InboundMessage.Type.NOTIFICATION) {
            LOG.info("WEBSOCKET_CLIENT: Received {} {}",
                kv("result", message.getType()),
                kv("correlation-id", message.getCorrelationId()));

            notificationsManager.onNotification(message.getMessage(), message.getQualifier());
        } else {
            LOG.warn("WEBSOCKET_CLIENT: Invalid InboundMessage {}", message);
        }
    }
}
