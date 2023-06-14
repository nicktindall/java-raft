package au.id.tindall.distalg.raft.state;

import java.io.Serializable;
import java.nio.ByteBuffer;

public interface IDSerializer<I extends Serializable> {

    I deserialize(ByteBuffer buf);

    ByteBuffer serialize(I id);
}
