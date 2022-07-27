package au.id.tindall.distalg.raft.state;

import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.ConfigurationEntry;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

public interface Snapshot extends Closeable {

    int getLastIndex();

    Term getLastTerm();

    long getLength();

    ConfigurationEntry getLastConfig();

    int readInto(ByteBuffer byteBuffer, int fromOffset);

    int writeBytes(int offset, byte[] chunk);

    int snapshotOffset();

    void snapshotOffset(int snapshotOffset);

    void finaliseSessions();

    void finalise();

    @Override
    void close() throws IOException;

    void delete();
}
