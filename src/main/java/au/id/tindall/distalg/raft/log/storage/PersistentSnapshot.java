package au.id.tindall.distalg.raft.log.storage;

import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.ConfigurationEntry;
import au.id.tindall.distalg.raft.serialisation.ByteBufferIO;
import au.id.tindall.distalg.raft.serialisation.IDSerializer;
import au.id.tindall.distalg.raft.serialisation.StreamingInput;
import au.id.tindall.distalg.raft.serialisation.StreamingOutput;
import au.id.tindall.distalg.raft.state.Snapshot;
import au.id.tindall.distalg.raft.util.IOUtil;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import static org.apache.logging.log4j.status.StatusLogger.getLogger;

public class PersistentSnapshot implements Snapshot, Closeable {

    private static final Logger LOGGER = getLogger();
    private static final String HEADER = "JavaSnapshot";
    private static final int LAST_INDEX_OFFSET = HEADER.length();
    private static final int LAST_TERM_OFFSET = LAST_INDEX_OFFSET + 4;
    private static final int STATE_ORDINAL_OFFSET = LAST_TERM_OFFSET + 4;
    private static final int CONTENTS_START_OFFSET = STATE_ORDINAL_OFFSET + 4;
    private static final int CONTENTS_LENGTH_OFFSET = CONTENTS_START_OFFSET + 4;
    private static final int DIGEST_OFFSET = CONTENTS_LENGTH_OFFSET + 8;
    private static final int SNAPSHOT_OFFSET_OFFSET = DIGEST_OFFSET + 16;
    private static final int CONFIG_OFFSET = SNAPSHOT_OFFSET_OFFSET + 4;
    public static final int HEADER_BUFFER_LENGTH = 4096;

    private final FileChannel fileChannel;
    private final int lastIndex;
    private final Term lastTerm;
    private final ConfigurationEntry lastConfig;
    private final int contentsStartOffset;
    private final Path path;
    private long contentsLength;
    private int snapshotOffset;
    private State state = State.INITIALISED;

    private PersistentSnapshot(Path path, FileChannel fileChannel, int lastIndex, Term lastTerm, ConfigurationEntry lastConfig, int contentsStartOffset) {
        this.path = path;
        this.fileChannel = fileChannel;
        this.lastIndex = lastIndex;
        this.lastTerm = lastTerm;
        this.lastConfig = lastConfig;
        this.contentsStartOffset = contentsStartOffset;
    }

    private PersistentSnapshot(Path path, FileChannel fileChannel, int lastIndex, Term lastTerm, ConfigurationEntry lastConfig, int contentsStartOffset,
                               long contentsLength) {
        this.path = path;
        this.fileChannel = fileChannel;
        this.lastIndex = lastIndex;
        this.lastTerm = lastTerm;
        this.lastConfig = lastConfig;
        this.contentsStartOffset = contentsStartOffset;
        this.contentsLength = contentsLength;
        this.state = State.COMPLETE;
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
        try {
            return fileChannel.read(byteBuffer, (long) contentsStartOffset + fromOffset);
        } catch (IOException e) {
            throw new IllegalStateException("Unable to read snapshot", e);
        }
    }

    @Override
    public int writeBytes(int offset, byte[] chunk) {
        try {
            return fileChannel.write(ByteBuffer.wrap(chunk), (long) contentsStartOffset + offset);
        } catch (IOException e) {
            throw new IllegalStateException("Unable to write snapshot", e);
        }
    }

    @Override
    public int getSnapshotOffset() {
        return snapshotOffset;
    }

    @Override
    public void setSnapshotOffset(int snapshotOffset) {
        try {
            IOUtil.writeInteger(fileChannel, SNAPSHOT_OFFSET_OFFSET, snapshotOffset);
        } catch (IOException e) {
            LOGGER.error("Error persisting snapshot offset");
        }
        this.snapshotOffset = snapshotOffset;
    }

    @Override
    public void finaliseSessions() {
        try {
            setSnapshotOffset((int) fileChannel.size() - contentsStartOffset);
        } catch (IOException e) {
            LOGGER.error("Error recording snapshot offset");
        }
    }

    @Override
    public void finalise() throws IOException {
        try {
            ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
            MessageDigest digest = MessageDigest.getInstance("MD5");
            int index = 0;
            while (true) {
                final int read = fileChannel.read(byteBuffer, index);
                if (read == 0) {
                    break;
                }
                digest.update(byteBuffer);
                index += read;
            }
            IOUtil.writeByte(fileChannel, STATE_ORDINAL_OFFSET, (byte) State.COMPLETE.ordinal());
            fileChannel.write(ByteBuffer.wrap(digest.digest()), DIGEST_OFFSET);
            contentsLength = fileChannel.size() - contentsStartOffset;
            IOUtil.writeLong(fileChannel, CONTENTS_LENGTH_OFFSET, contentsLength);
            state = State.COMPLETE;
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("Couldn't load MD5 algorithm, this should never happen", e);
        }
    }

    @Override
    public void close() throws IOException {
        fileChannel.close();
    }

    @Override
    public void delete() {
        try {
            close();
            Files.deleteIfExists(path);
        } catch (IOException e) {
            LOGGER.warn("Error deleting snapshot, file may be left", e);
        }
    }

    @Override
    public long getLength() {
        return contentsLength;
    }

    public Path path() {
        return path;
    }

    enum State {
        INITIALISED,
        COMPLETE
    }

    @SuppressWarnings("java:S2095")
    public static PersistentSnapshot create(IDSerializer idSerializer, Path path, int lastIndex, Term lastTerm, ConfigurationEntry configurationEntry) throws IOException {
        ByteBuffer byteBuffer = ByteBuffer.allocate(HEADER_BUFFER_LENGTH);
        byteBuffer.put(ByteBuffer.wrap(HEADER.getBytes(StandardCharsets.UTF_8)));
        byteBuffer.putInt(LAST_INDEX_OFFSET, lastIndex);
        byteBuffer.putInt(LAST_TERM_OFFSET, lastTerm.getNumber());
        byteBuffer.put(STATE_ORDINAL_OFFSET, (byte) PersistentSnapshot.State.INITIALISED.ordinal());
        byteBuffer.position(CONFIG_OFFSET);
        writeConfigurationEntry(idSerializer, byteBuffer, configurationEntry);
        final int contentsStartIndex = byteBuffer.position();
        byteBuffer.putInt(CONTENTS_START_OFFSET, contentsStartIndex);
        FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, StandardOpenOption.READ);
        byteBuffer.flip();
        fileChannel.write(byteBuffer);
        return new PersistentSnapshot(path, fileChannel, lastIndex, lastTerm, configurationEntry, contentsStartIndex);
    }

    @SuppressWarnings("java:S2095")
    public static PersistentSnapshot load(IDSerializer idSerializer, Path path) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(HEADER_BUFFER_LENGTH);
        FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.READ);
        fileChannel.read(buffer);
        buffer.position(0);
        buffer.limit(HEADER.length());
        if (!ByteBuffer.wrap(HEADER.getBytes(StandardCharsets.UTF_8)).equals(buffer)) {
            throw new IllegalArgumentException("Invalid snapshot file, got header: " + buffer);
        }
        buffer.limit(HEADER_BUFFER_LENGTH);
        int lastIndex = buffer.getInt(LAST_INDEX_OFFSET);
        Term lastTerm = new Term(buffer.getInt(LAST_TERM_OFFSET));
        long contentsLength = buffer.getLong(CONTENTS_LENGTH_OFFSET);
        ConfigurationEntry entry = readConfigurationEntry(idSerializer, buffer);
        final PersistentSnapshot persistentSnapshot = new PersistentSnapshot(path, fileChannel, lastIndex, lastTerm, entry, buffer.getInt(CONTENTS_START_OFFSET), contentsLength);
        persistentSnapshot.snapshotOffset = buffer.getInt(SNAPSHOT_OFFSET_OFFSET);
        return persistentSnapshot;
    }

    private static void writeConfigurationEntry(IDSerializer idSerializer, ByteBuffer buffer, ConfigurationEntry entry) {
        final ByteBufferIO byteBufferIO = ByteBufferIO.wrap(idSerializer, buffer);
        byteBufferIO.writeNullable(entry, StreamingOutput::writeStreamable);
    }

    private static ConfigurationEntry readConfigurationEntry(IDSerializer idSerializer, ByteBuffer buffer) {
        final ByteBufferIO byteBufferIO = ByteBufferIO.wrap(idSerializer, buffer);
        byteBufferIO.setReadPosition(CONFIG_OFFSET);
        return byteBufferIO.readNullable(StreamingInput::readStreamable);
    }
}
