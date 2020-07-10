package pbouda.netty.websocket.client;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.websocketx.WebSocketClientProtocolHandler;
import io.netty.handler.ssl.SslCompletionEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class EventHandler extends ChannelInboundHandlerAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(EventHandler.class);

    private final CountDownLatch activated = new CountDownLatch(1);

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object event) {
        if (event instanceof WebSocketClientProtocolHandler.ClientHandshakeStateEvent) {
            LOG.info("WEBSOCKET_CLIENT: Websocket Handshake event: " + event);

            if (event == WebSocketClientProtocolHandler.ClientHandshakeStateEvent.HANDSHAKE_COMPLETE) {
                this.activated.countDown();
            }
        } else if (event instanceof SslCompletionEvent) {
            SslCompletionEvent sslEvent = (SslCompletionEvent) event;
            if (sslEvent.cause() == null) {
                LOG.info("WEBSOCKET_CLIENT: SSL Success Handshake event: {}",
                    sslEvent.getClass().getSimpleName());
            } else {
                LOG.error("WEBSOCKET_CLIENT: SSL Handshake event: {}",
                    sslEvent.getClass().getSimpleName(), sslEvent.cause());
            }
        }
        ctx.fireUserEventTriggered(event);
    }

    /**
     * Waits until this handler is ready for handling messages.
     * That means that SSL and WebSocket handshakes are done.
     *
     * @param timeout time to wait for an activation flag.
     */
    public void waitForActivation(Duration timeout) {
        try {
            activated.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.error("A thread interruption occurred while waiting for an activation", e);
        }
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext context) {
        if (context.channel().isWritable()) {
            LOG.info("WEBSOCKET_CLIENT: Channel '{}' became writable again",
                context.channel().remoteAddress());
        } else {
            LOG.error("WEBSOCKET_CLIENT: Channel '{}' became not writable (probably slower consumer)",
                context.channel().remoteAddress());
        }
        context.fireChannelWritabilityChanged();
    }
}
