package au.id.tindall.distalg.raft.serialisation;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

public interface StreamingOutput {

    default void writeMessageIdentifier(MessageIdentifier messageIdentifier) {
        writeString(messageIdentifier.getMessageType());
    }

    default <E extends Enum<E>> void writeEnum(E enumeration) {
        writeString(enumeration.name());
    }

    void writeBoolean(boolean value);

    void writeInteger(int value);

    void writeLong(long value);

    void writeDouble(double value);

    void writeString(String value);

    void writeBytes(byte[] value);

    void writeIdentifier(Object value);

    default <T> void writeSet(Set<T> set, BiConsumer<StreamingOutput, T> valueWriter) {
        this.writeInteger(set.size());
        set.forEach(value -> valueWriter.accept(this, value));
    }

    default <K, V> void writeMap(Map<K, V> map, BiConsumer<StreamingOutput, K> keyWriter, BiConsumer<StreamingOutput, V> valueWriter) {
        this.writeInteger(map.size());
        for (Map.Entry<K, V> entry : map.entrySet()) {
            keyWriter.accept(this, entry.getKey());
            valueWriter.accept(this, entry.getValue());
        }
    }

    default <T> void writeList(List<T> list, BiConsumer<StreamingOutput, T> writer) {
        this.writeInteger(list.size());
        list.forEach(item -> writer.accept(this, item));
    }

    default <T> void writeNullable(T thing, BiConsumer<StreamingOutput, T> writer) {
        this.writeBoolean(thing != null);
        if (thing != null) {
            writer.accept(this, thing);
        }
    }

    default void writeStreamable(Streamable streamable) {
        writeMessageIdentifier(streamable.getMessageIdentifier());
        streamable.writeTo(this);
    }
}
