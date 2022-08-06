package au.id.tindall.distalg.raft.log.storage;

import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.ConfigurationEntry;
import au.id.tindall.distalg.raft.state.Snapshot;
import au.id.tindall.distalg.raft.util.IOUtil;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
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
    private static final int CONFIG_LENGTH_OFFSET = STATE_ORDINAL_OFFSET + 4;
    private static final int CONTENTS_START_OFFSET = CONFIG_LENGTH_OFFSET + 4;
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
    private State state = State.Initialised;

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
        this.state = State.Complete;
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
            return fileChannel.read(byteBuffer, contentsStartOffset + fromOffset);
        } catch (IOException e) {
            throw new IllegalStateException("Unable to read snapshot", e);
        }
    }

    @Override
    public int writeBytes(int offset, byte[] chunk) {
        try {
            return fileChannel.write(ByteBuffer.wrap(chunk), contentsStartOffset + offset);
        } catch (IOException e) {
            throw new IllegalStateException("Unable to write snapshot", e);
        }
    }

    @Override
    public int snapshotOffset() {
        return snapshotOffset;
    }

    @Override
    public void snapshotOffset(int snapshotOffset) {
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
            snapshotOffset((int) fileChannel.size() - contentsStartOffset);
        } catch (IOException e) {
            LOGGER.error("Error recording snapshot offset");
        }
    }

    @Override
    public void finalise() {
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
            IOUtil.writeByte(fileChannel, STATE_ORDINAL_OFFSET, (byte) State.Complete.ordinal());
            fileChannel.write(ByteBuffer.wrap(digest.digest()), DIGEST_OFFSET);
            contentsLength = fileChannel.size() - contentsStartOffset;
            IOUtil.writeLong(fileChannel, CONTENTS_LENGTH_OFFSET, contentsLength);
            state = State.Complete;
        } catch (IOException e) {
            throw new RuntimeException("Error calculating digest", e);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("This won't happen", e);
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
        } catch (IOException e) {
            LOGGER.warn("Error closing fileChannel", e);

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
        Initialised,
        Complete
    }

    public static PersistentSnapshot create(Path path, int lastIndex, Term lastTerm, ConfigurationEntry configurationEntry) {
        try {
            byte[] configEntry = serializeConfigEntry(configurationEntry);
            ByteBuffer byteBuffer = ByteBuffer.allocate(HEADER_BUFFER_LENGTH);
            byteBuffer.put(ByteBuffer.wrap(HEADER.getBytes(StandardCharsets.UTF_8)));
            byteBuffer.putInt(LAST_INDEX_OFFSET, lastIndex);
            byteBuffer.putInt(LAST_TERM_OFFSET, lastTerm.getNumber());
            byteBuffer.put(STATE_ORDINAL_OFFSET, (byte) PersistentSnapshot.State.Initialised.ordinal());
            byteBuffer.putInt(CONFIG_LENGTH_OFFSET, configEntry.length); // Length
            byteBuffer.position(CONFIG_OFFSET);
            byteBuffer.put(configEntry);
            final int contentsStartIndex = byteBuffer.position();
            byteBuffer.putInt(CONTENTS_START_OFFSET, contentsStartIndex);
            FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, StandardOpenOption.READ);
            byteBuffer.flip();
            fileChannel.write(byteBuffer);
            return new PersistentSnapshot(path, fileChannel, lastIndex, lastTerm, configurationEntry, contentsStartIndex);
        } catch (IOException e) {
            throw new RuntimeException("Error creating snapshot", e);
        }
    }

    public static PersistentSnapshot load(Path path) {
        try {
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
            int configLength = buffer.getInt(CONFIG_LENGTH_OFFSET);
            long contentsLength = buffer.getLong(CONTENTS_LENGTH_OFFSET);
            ConfigurationEntry entry;
            byte[] configBytesArray = new byte[configLength];
            buffer.position(CONFIG_OFFSET);
            buffer.get(configBytesArray);
            try (final ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(configBytesArray))) {
                entry = (ConfigurationEntry) ois.readObject();
            }
            final PersistentSnapshot persistentSnapshot = new PersistentSnapshot(path, fileChannel, lastIndex, lastTerm, entry, buffer.getInt(CONTENTS_START_OFFSET), contentsLength);
            persistentSnapshot.snapshotOffset = buffer.getInt(SNAPSHOT_OFFSET_OFFSET);
            return persistentSnapshot;
        } catch (ClassNotFoundException | IOException e) {
            throw new RuntimeException("Error loading snapshot", e);
        }
    }

    private static byte[] serializeConfigEntry(ConfigurationEntry configurationEntry) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(configurationEntry);
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
