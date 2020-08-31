package au.id.tindall.distalg.raft.state;

import java.io.Serializable;
import java.nio.ByteBuffer;

public interface IDSerializer<ID extends Serializable> {

    ID deserialize(ByteBuffer buf);

    ByteBuffer serialize(ID id);
}
