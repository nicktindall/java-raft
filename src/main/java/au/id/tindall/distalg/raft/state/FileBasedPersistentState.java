package au.id.tindall.distalg.raft.state;

import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.storage.LogStorage;
import au.id.tindall.distalg.raft.log.storage.PersistentLogStorage;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;

import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.SYNC;
import static java.nio.file.StandardOpenOption.WRITE;

/**
 * Very crude file-based persistent state. Layout of state file is:
 *
 * - length of serialized ID (integer)
 * - current term (integer)
 * - length of serialized voted-for ID (set to zero if absent, integer)
 * - serialized ID
 * - serialized voted for ID (if present)
 * - EOF
 */
public class FileBasedPersistentState<ID extends Serializable> implements PersistentState<ID> {

    private static final long START_OF_ID_LENGTH = 0L;
    private static final long START_OF_CURRENT_TERM = 4L;
    private static final long START_OF_VOTED_FOR_LENGTH = 8L;
    private static final long START_OF_ID = 12L;
    private final LogStorage logStorage;
    private final FileChannel fileChannel;
    private final IDSerializer<ID> idSerializer;

    private ID id;
    private Term currentTerm;
    private ID votedFor;

    public static <ID extends Serializable> FileBasedPersistentState<ID> create(Path stateFilesPrefix, ID serverId) {
        PersistentLogStorage persistentLogStorage = new PersistentLogStorage(logFilePath(stateFilesPrefix));
        return new FileBasedPersistentState<>(persistentLogStorage, stateFilePath(stateFilesPrefix), new JavaIDSerializer<>(), serverId);
    }

    public static <ID extends Serializable> FileBasedPersistentState<ID> createOrOpen(Path stateFilesPrefix, ID serverId) throws IOException {
        Path logFilePath = logFilePath(stateFilesPrefix);
        Path stateFilePath = stateFilePath(stateFilesPrefix);
        if (Files.exists(logFilePath) && Files.exists(stateFilePath)) {
            PersistentLogStorage persistentLogStorage = new PersistentLogStorage(logFilePath);
            return new FileBasedPersistentState<>(persistentLogStorage, stateFilePath, new JavaIDSerializer<>());
        } else {
            Files.deleteIfExists(logFilePath);
            Files.deleteIfExists(stateFilePath);
            PersistentLogStorage persistentLogStorage = new PersistentLogStorage(logFilePath);
            return new FileBasedPersistentState<>(persistentLogStorage, stateFilePath, new JavaIDSerializer<>(), serverId);
        }
    }

    private static Path stateFilePath(Path stateFilesPrefix) {
        return stateFilesPrefix.resolveSibling(stateFilesPrefix.getFileName() + ".state");
    }

    private static Path logFilePath(Path stateFilesPrefix) {
        return stateFilesPrefix.resolveSibling(stateFilesPrefix.getFileName() + ".log");
    }

    /**
     * Create a new file-based persistent state
     */
    public FileBasedPersistentState(LogStorage logStorage, Path path, IDSerializer<ID> idSerializer, ID id) {
        try {
            this.logStorage = logStorage;
            this.fileChannel = FileChannel.open(path, CREATE_NEW, READ, WRITE, SYNC);
            this.idSerializer = idSerializer;
            this.id = id;
            this.currentTerm = new Term(0);
            writeToStateFile();
        } catch (IOException e) {
            throw new RuntimeException("Unable to create new state file", e);
        }
    }

    /**
     * Load an existing file-based persistent state
     */
    public FileBasedPersistentState(LogStorage logStorage, Path path, IDSerializer<ID> idSerializer) {
        try {
            this.logStorage = logStorage;
            this.fileChannel = FileChannel.open(path, READ, WRITE, SYNC);
            this.idSerializer = idSerializer;
            readFromStateFile();
        } catch (IOException e) {
            throw new RuntimeException("Unable to open existing state file", e);
        }
    }

    private void readFromStateFile() {
        try {
            int idLength = readIntFrom(START_OF_ID_LENGTH);
            currentTerm = new Term(readIntFrom(START_OF_CURRENT_TERM));
            int votedForLength = readIntFrom(START_OF_VOTED_FOR_LENGTH);
            id = readIdFrom(START_OF_ID, idLength);
            votedFor = readIdFrom(START_OF_ID + idLength, votedForLength);
        } catch (IOException e) {
            throw new RuntimeException("Error reading from state file", e);
        }
    }

    private int readIntFrom(long startPoint) throws IOException {
        ByteBuffer intBuffer = ByteBuffer.allocate(4);
        fileChannel.read(intBuffer, startPoint);
        intBuffer.position(0);
        return intBuffer.getInt();
    }

    private ID readIdFrom(long startPoint, int length) throws IOException {
        if (length == 0) {
            return null;
        }
        ByteBuffer idBuffer = ByteBuffer.allocate(length);
        fileChannel.read(idBuffer, startPoint);
        idBuffer.position(0);
        return idSerializer.deserialize(idBuffer);
    }

    private void writeToStateFile() {
        try {
            ByteBuffer idBytes = idSerializer.serialize(id);
            ByteBuffer votedForBytes = votedFor != null ? idSerializer.serialize(votedFor) : ByteBuffer.allocate(0);
            writeIntTo(START_OF_ID_LENGTH, idBytes.capacity());
            writeIntTo(START_OF_CURRENT_TERM, currentTerm.getNumber());
            writeIntTo(START_OF_VOTED_FOR_LENGTH, votedForBytes.capacity());
            fileChannel.write(idBytes, START_OF_ID);
            fileChannel.write(votedForBytes, START_OF_ID + idBytes.capacity());
            fileChannel.truncate(START_OF_ID + idBytes.capacity() + votedForBytes.capacity());
        } catch (IOException e) {
            throw new RuntimeException("Error writing to state file");
        }
    }

    private void writeIntTo(long startPoint, int value) throws IOException {
        ByteBuffer intBuffer = ByteBuffer.allocate(4);
        intBuffer.putInt(value);
        intBuffer.position(0);
        fileChannel.write(intBuffer, startPoint);
    }

    @Override
    public ID getId() {
        return id;
    }

    @Override
    public void setCurrentTerm(Term term) {
        if (term.isGreaterThan(this.currentTerm)) {
            this.currentTerm = term;
            this.votedFor = null;
            writeToStateFile();
        } else if (term.isLessThan(this.currentTerm)) {
            throw new IllegalArgumentException("Attempted to reduce current term!");
        }
    }

    @Override
    public Term getCurrentTerm() {
        return this.currentTerm;
    }

    @Override
    public void setVotedFor(ID votedFor) {
        if (!Objects.equals(this.votedFor, votedFor)) {
            this.votedFor = votedFor;
            writeToStateFile();
        }
    }

    @Override
    public Optional<ID> getVotedFor() {
        return Optional.ofNullable(this.votedFor);
    }

    @Override
    public LogStorage getLogStorage() {
        return logStorage;
    }
}
