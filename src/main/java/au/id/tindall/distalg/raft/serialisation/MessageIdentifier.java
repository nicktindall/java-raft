package au.id.tindall.distalg.raft.serialisation;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class MessageIdentifier {

    private static final Map<String, MessageIdentifier> REGISTERED_MESSAGES = new ConcurrentHashMap<>();
    private final String messageType;
    private final Class<? extends Streamable> messageClass;
    private final Function<StreamingInput, Streamable> messageParser;

    private MessageIdentifier(String messageType, Class<? extends Streamable> messageClass) {
        this.messageType = messageType;
        this.messageClass = messageClass;
        this.messageParser = getMessageParser(messageClass);
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

    public <T extends Streamable> T parseMessage(StreamingInput streamingInput) {
        return (T) messageParser.apply(streamingInput);
    }

    private static Function<StreamingInput, Streamable> getMessageParser(Class<? extends Streamable> messageClass) {
        // Look for @Parser annotated static method...
        for (Method method : messageClass.getMethods()) {
            if (Modifier.isStatic(method.getModifiers())
                    && method.getAnnotation(Parser.class) != null
                    && method.getParameterCount() == 1
                    && method.getParameterTypes()[0].equals(StreamingInput.class)) {
                return streamingInput -> {
                    try {
                        return (Streamable) method.invoke(null, streamingInput);
                    } catch (IllegalAccessException | InvocationTargetException e) {
                        throw new IllegalStateException("Error invoking parser method", e);
                    }
                };
            }
        }
        // ... or an appropriate constructor
        try {
            Constructor<?> constructor = messageClass.getDeclaredConstructor(StreamingInput.class);
            return streamingInput -> {
                try {
                    return (Streamable) constructor.newInstance(streamingInput);
                } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
                    throw new IllegalStateException("Error invoking parser constructor", e);
                }
            };
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException("No streaming constructor. class=" + messageClass.getName(), e);
        }
    }
}
