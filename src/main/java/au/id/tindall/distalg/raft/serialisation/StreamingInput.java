package au.id.tindall.distalg.raft.serialisation;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.IntFunction;

public interface StreamingInput {

    default MessageIdentifier readMessageIdentifier() {
        return MessageIdentifier.lookupMessageIdentifier(readString());
    }

    default <E extends Enum<E>> E readEnum(Class<E> enumClass) {
        return Enum.valueOf(enumClass, readString());
    }

    boolean readBoolean();

    int readInteger();

    long readLong();

    double readDouble();

    String readString();

    byte[] readBytes();

    <T> T readIdentifier();

    default <T> Set<T> readSet(IntFunction<Set<T>> setSupplier, Function<StreamingInput, T> valueReader) {
        int numEntries = readInteger();
        final Set<T> set = numEntries > 0 ? setSupplier.apply(numEntries) : Collections.emptySet();
        for (int i = 0; i < numEntries; i++) {
            T value = valueReader.apply(this);
            set.add(value);
        }
        return set;
    }

    default <K, V> Map<K, V> readMap(IntFunction<Map<K, V>> mapSupplier, Function<StreamingInput, K> keyWriter, Function<StreamingInput, V> valueWriter) {
        int numEntries = readInteger();
        final Map<K, V> map = numEntries > 0 ? mapSupplier.apply(numEntries) : Collections.emptyMap();
        for (int i = 0; i < numEntries; i++) {
            K key = keyWriter.apply(this);
            V value = valueWriter.apply(this);
            map.put(key, value);
        }
        return map;
    }

    default <T> List<T> readList(IntFunction<List<T>> listSupplier, Function<StreamingInput, T> reader) {
        final int numItems = this.readInteger();
        final List<T> list = numItems > 0 ? listSupplier.apply(numItems) : Collections.emptyList();
        for (int i = 0; i < numItems; i++) {
            list.add(reader.apply(this));
        }
        return list;
    }

    default <T> T readNullable(Function<StreamingInput, T> reader) {
        boolean isPresent = this.readBoolean();
        if (isPresent) {
            return reader.apply(this);
        }
        return null;
    }

    default <T extends Streamable> T readStreamable() {
        final Class<T> streamableClass = readMessageIdentifier().getMessageClass();
        try {
            Constructor<T> constructor = streamableClass.getDeclaredConstructor(StreamingInput.class);
            return constructor.newInstance(this);
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException("No streaming constructor. class=" + streamableClass.getName(), e);
        } catch (InvocationTargetException | InstantiationException | IllegalAccessException e) {
            throw new IllegalStateException("Error deserializing streamable, class=" + streamableClass.getName(), e);
        }
    }
}
