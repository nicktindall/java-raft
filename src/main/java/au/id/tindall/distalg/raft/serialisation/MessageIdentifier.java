package au.id.tindall.distalg.raft.serialisation;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MessageIdentifier {

    private static final Map<String, MessageIdentifier> REGISTERED_MESSAGES = new ConcurrentHashMap<>();
    private final String messageType;
    private final Class<? extends Streamable> messageClass;

    private MessageIdentifier(String messageType, Class<? extends Streamable> messageClass) {
        this.messageType = messageType;
        this.messageClass = messageClass;
    }

    public static MessageIdentifier registerMessageIdentifier(String messageType, Class<? extends Streamable> streamable) {
        return REGISTERED_MESSAGES.compute(messageType, (id, existing) -> {
            if (existing != null) {
                throw new IllegalStateException("Message identifier is already registered. Existing type=" + existing);
            }
            return new MessageIdentifier(messageType, streamable);
        });
    }

    public static MessageIdentifier lookupMessageIdentifier(String messageType) {
        MessageIdentifier messageRegistration = REGISTERED_MESSAGES.get(messageType);
        if (messageRegistration == null) {
            throw new IllegalStateException("Message identifier is not registered. MessageType=" + messageType);
        }
        return messageRegistration;
    }

    public String getMessageType() {
        return messageType;
    }

    @SuppressWarnings("unchecked")
    public <T extends Streamable> Class<T> getMessageClass() {
        return (Class<T>) messageClass;
    }
}
