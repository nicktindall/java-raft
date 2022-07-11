package au.id.tindall.distalg.raft.state;

import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.ConfigurationEntry;

import java.nio.ByteBuffer;

public class InMemorySnapshot implements Snapshot {

    // TODO server should send this
    private static final int SNAPSHOT_SIZE = 1 << 19; // about 512k

    private final int lastIndex;
    private final Term lastTerm;
    private final ConfigurationEntry lastConfig;
    private final ByteBuffer contents;

    public InMemorySnapshot(int lastIndex, Term lastTerm, ConfigurationEntry lastConfig) {
        this.lastIndex = lastIndex;
        this.lastTerm = lastTerm;
        this.lastConfig = lastConfig;
        this.contents = ByteBuffer.allocate(SNAPSHOT_SIZE);
    }

    @Override
    public int getLastIndex() {
        return lastIndex;
    }

    @Override
    public Term getLastTerm() {
        return lastTerm;
    }

    @Override
    public ConfigurationEntry getLastConfig() {
        return lastConfig;
    }

    @Override
    public synchronized int readInto(ByteBuffer byteBuffer, int fromOffset) {
        int startPosition = byteBuffer.position();
        byteBuffer.put(contents.position(fromOffset));
        return byteBuffer.position() - startPosition;
    }

    @Override
    public synchronized int writeBytes(int offset, byte[] chunk) {
        int bytesWritten = chunk.length;
        contents.position(offset).put(chunk);
        return bytesWritten;
    }

    @Override
    public void finalise() {
        // Do nothing
    }

    @Override
    public void close() {
        // Do nothing
    }
}
