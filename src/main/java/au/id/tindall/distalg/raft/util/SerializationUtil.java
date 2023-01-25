package au.id.tindall.distalg.raft.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public enum SerializationUtil {
    ;

    public static byte[] serializeObject(Object object) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(object);
            return baos.toByteArray();
        } catch (IOException e) {
            throw new IllegalArgumentException("Error serializing an object", e);
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> T deserializeObject(byte[] bytes) {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
             ObjectInputStream oos = new ObjectInputStream(bais)) {
            return (T) oos.readObject();
        } catch (ClassNotFoundException | IOException e) {
            throw new IllegalArgumentException("Error deserializing an object", e);
        }
    }
}
