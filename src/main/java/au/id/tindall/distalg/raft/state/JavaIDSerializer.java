package au.id.tindall.distalg.raft.state;

import java.io.Serializable;
import java.nio.ByteBuffer;

import static au.id.tindall.distalg.raft.util.SerializationUtil.deserializeObject;
import static au.id.tindall.distalg.raft.util.SerializationUtil.serializeObject;

public class JavaIDSerializer<I extends Serializable> implements IDSerializer<I> {

    @Override
    public I deserialize(ByteBuffer buf) {
        return deserializeObject(buf.array());
    }

    @Override
    public ByteBuffer serialize(I id) {
        return ByteBuffer.wrap(serializeObject(id));
    }
}
