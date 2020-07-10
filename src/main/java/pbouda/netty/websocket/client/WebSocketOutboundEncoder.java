package pbouda.netty.websocket.client;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;

import java.util.List;

@ChannelHandler.Sharable
public class WebSocketOutboundEncoder extends MessageToMessageEncoder<OutboundMessage> {

    public WebSocketOutboundEncoder() {
        super(OutboundMessage.class);
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, OutboundMessage msg, List<Object> out) {
        String message = msg.getMessage();
        out.add(new TextWebSocketFrame(message));
    }
}