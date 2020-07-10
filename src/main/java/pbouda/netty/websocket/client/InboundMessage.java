package pbouda.netty.websocket.client;

import com.fasterxml.jackson.databind.JsonNode;

public class InboundMessage {

    private final String correlationId;
    private final String message;
    private final Type type;
    private final String qualifier;

    private String tag;
    private long processingTimestamp;

    private InboundMessage(String correlationId, String message, Type type) {
        this(correlationId, message, type, null);
    }

    private InboundMessage(String correlationId, String message, Type type, String qualifier) {
        this.correlationId = correlationId;
        this.message = message;
        this.type = type;
        this.qualifier = qualifier;
    }

    /**
     * Unfortunate parsing of messages from IMS.
     *
     * @param message a raw incoming message.
     * @return a parsed message and properly typed according to a content.
     */
    public static InboundMessage parse(String message) {
        JsonNode rootNode = JsonMapper.readTree(message);
        String correlationId = rootNode.findValue("correlationId").asText();
        String qualifier = rootNode.findValue("qualifier").asText();

        if (qualifier.endsWith("Notification")) {
            return new InboundMessage(correlationId, message, Type.NOTIFICATION, qualifier);
        } else if (qualifier.endsWith("QuickLinkErrorResponse")) {
            return new InboundMessage(correlationId, message, Type.ERROR, qualifier);
        } else {
            return new InboundMessage(correlationId, message, Type.SUCCESS);
        }
    }

    public static InboundMessage timeout(String correlationId) {
        return new InboundMessage(correlationId, null, Type.TIMEOUT);
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public String getMessage() {
        return message;
    }

    public Type getType() {
        return type;
    }

    public String getQualifier() {
        return qualifier;
    }

    public String getTag() {
        return tag;
    }

    public long getProcessingTimestamp() {
        return processingTimestamp;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public void setProcessingTimestamp(long processingTimestamp) {
        this.processingTimestamp = processingTimestamp;
    }

    public enum Type {
        SUCCESS, ERROR, TIMEOUT, NOTIFICATION
    }

    @Override
    public String toString() {
        return "InboundMessage{" +
               "correlationId='" + correlationId + '\'' +
               ", message='" + message + '\'' +
               ", type=" + type +
               ", qualifier='" + qualifier + '\'' +
               '}';
    }
}
