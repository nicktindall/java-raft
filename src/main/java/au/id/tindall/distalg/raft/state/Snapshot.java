package au.id.tindall.distalg.raft.state;

import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.ConfigurationEntry;

import java.nio.ByteBuffer;

public interface Snapshot {

    int getLastIndex();

    Term getLastTerm();

    ConfigurationEntry getLastConfig();

    void readInto(ByteBuffer byteBuffer, int fromOffset);

    int writeBytes(int offset, ByteBuffer byteBuffer);
}
