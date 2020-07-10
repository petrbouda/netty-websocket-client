package pbouda.netty.websocket.client;

public class OutboundMessage {
    private final String correlationId;
    private final String message;
    private final ResponsePublisher publisher;

    public OutboundMessage(String correlationId, String message, ResponsePublisher publisher) {
        this.correlationId = correlationId;
        this.message = message;
        this.publisher = publisher;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public String getMessage() {
        return message;
    }

    public ResponsePublisher getPublisher() {
        return publisher;
    }
}
