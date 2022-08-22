package au.id.tindall.distalg.raft.state;

import java.io.Serializable;
import java.nio.ByteBuffer;

import static au.id.tindall.distalg.raft.util.SerializationUtil.deserializeObject;
import static au.id.tindall.distalg.raft.util.SerializationUtil.serializeObject;

public class JavaIDSerializer<ID extends Serializable> implements IDSerializer<ID> {

    @Override
    @SuppressWarnings("unchecked")
    public ID deserialize(ByteBuffer buf) {
        return deserializeObject(buf.array());
    }

    @Override
    public ByteBuffer serialize(ID id) {
        return ByteBuffer.wrap(serializeObject(id));
    }
}
