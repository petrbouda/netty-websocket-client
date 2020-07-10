package pbouda.netty.websocket.client;

public abstract class WsException extends RuntimeException {
    private final InboundMessage inboundMessage;

    public WsException(InboundMessage inboundMessage) {
        this.inboundMessage = inboundMessage;
    }

    public InboundMessage getInboundMessage() {
        return inboundMessage;
    }

    abstract public ErrorResponse toError();

    public static class Error extends WsException {
        public Error(InboundMessage message) {
            super(message);
        }

        @Override
        public ErrorResponse toError() {
            return JsonMapper.unmarshal(getInboundMessage().getMessage(), ErrorResponse.class);
        }
    }

    public static class Timeout extends WsException {
        public Timeout(InboundMessage message) {
            super(message);
        }

        @Override
        public ErrorResponse toError() {
            InboundMessage message = getInboundMessage();
            return new ErrorResponse(ResponseWrapper.IMS_TIMEOUT, message.getCorrelationId(),
                ErrorType.SystemError, "No response received from IMS", null);
        }
    }
}
