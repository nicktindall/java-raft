package au.id.tindall.distalg.raft.state;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;

public class JavaIDSerializer<ID extends Serializable> implements IDSerializer<ID> {

    @Override
    @SuppressWarnings("unchecked")
    public ID deserialize(ByteBuffer buf) {
        ByteArrayInputStream bais = new ByteArrayInputStream(buf.array());
        try (ObjectInputStream ois = new ObjectInputStream(bais)) {
            return (ID) ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException("Error deserializing ID", e);
        }
    }

    @Override
    public ByteBuffer serialize(ID id) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(id);
        } catch (IOException e) {
            throw new RuntimeException("Error serializing ID", e);
        }
        return ByteBuffer.wrap(baos.toByteArray());
    }
}
