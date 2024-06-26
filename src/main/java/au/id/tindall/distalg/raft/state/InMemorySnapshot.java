package au.id.tindall.distalg.raft.state;

import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.ConfigurationEntry;

import java.nio.ByteBuffer;

public class InMemorySnapshot implements Snapshot {

    private static final int INITIAL_SIZE = 4096;
    private static final int GROWTH_RATE = 2;

    private final int lastIndex;
    private final Term lastTerm;
    private final ConfigurationEntry lastConfig;
    private ByteBuffer contents;
    private int snapshotOffset;

    public InMemorySnapshot(int lastIndex, Term lastTerm, ConfigurationEntry lastConfig) {
        this.lastIndex = lastIndex;
        this.lastTerm = lastTerm;
        this.lastConfig = lastConfig;
        this.contents = ByteBuffer.allocate(INITIAL_SIZE);
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
    public int readInto(ByteBuffer byteBuffer, int fromOffset) {
        if (fromOffset > contents.limit()) {
            return 0;
        }
        int startPosition = byteBuffer.position();
        contents.position(fromOffset);
        final int amountToCopy = Math.min(byteBuffer.remaining(), contents.remaining());
        final int limit0 = contents.limit();
        try {
            contents.limit(contents.position() + amountToCopy);
            byteBuffer.put(contents);
        } finally {
            contents.limit(limit0);
        }
        return byteBuffer.position() - startPosition;
    }

    @Override
    public int writeBytes(int offset, byte[] chunk) {
        int bytesWritten = chunk.length;
        growBufferIfNecessary(offset + chunk.length);
        contents.position(offset).put(chunk);
        return bytesWritten;
    }

    @Override
    public int getSnapshotOffset() {
        return snapshotOffset;
    }

    @Override
    public void setSnapshotOffset(int snapshotOffset) {
        this.snapshotOffset = snapshotOffset;
    }

    @Override
    public void finaliseSessions() {
        snapshotOffset = contents.position();
    }

    private void growBufferIfNecessary(int lengthToAccommodate) {
        if (contents.capacity() < lengthToAccommodate) {
            final ByteBuffer grownContents = ByteBuffer.allocate(Math.max(lengthToAccommodate, contents.capacity() * GROWTH_RATE));
            contents.flip();
            grownContents.put(contents);
            contents = grownContents;
        }
    }

    @Override
    public void finalise() {
        contents.flip();
    }

    @Override
    public void close() {
        // Do nothing
    }

    @Override
    public void delete() {
        // Do nothing
    }

    @Override
    public long getLength() {
        return contents.array().length;
    }

    public ByteBuffer getContents() {
        return contents;
    }
}
