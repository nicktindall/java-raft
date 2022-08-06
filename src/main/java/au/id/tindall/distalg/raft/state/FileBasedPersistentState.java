package au.id.tindall.distalg.raft.state;

import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.ConfigurationEntry;
import au.id.tindall.distalg.raft.log.storage.LogStorage;
import au.id.tindall.distalg.raft.log.storage.PersistentLogStorage;
import au.id.tindall.distalg.raft.log.storage.PersistentSnapshot;
import au.id.tindall.distalg.raft.util.Closeables;
import au.id.tindall.distalg.raft.util.IOUtil;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.SYNC;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.apache.logging.log4j.LogManager.getLogger;

/**
 * Very crude file-based persistent state. Layout of state file is:
 * <p>
 * - length of serialized ID (integer)
 * - current term (integer)
 * - length of serialized voted-for ID (set to zero if absent, integer)
 * - serialized ID
 * - serialized voted for ID (if present)
 * - EOF
 */
public class FileBasedPersistentState<ID extends Serializable> implements PersistentState<ID> {

    private static final Logger LOGGER = getLogger();
    private static final long START_OF_ID_LENGTH = 0L;
    private static final long START_OF_CURRENT_TERM = 4L;
    private static final long START_OF_VOTED_FOR_LENGTH = 8L;
    private static final long START_OF_ID = 12L;
    private final LogStorage logStorage;
    private final FileChannel fileChannel;
    private final IDSerializer<ID> idSerializer;

    private ID id;
    private final AtomicReference<Term> currentTerm;
    private final AtomicReference<ID> votedFor;

    private final Function<Integer, Path> tempSnapshotPathGenerator;
    private final Path currentSnapshotPath;
    private final List<SnapshotInstalledListener> snapshotInstalledListeners;
    private AtomicReference<Snapshot> currentSnapshot = new AtomicReference<>();
    private AtomicInteger nextSnapshotSequence = new AtomicInteger(0);


    public static <ID extends Serializable> FileBasedPersistentState<ID> create(Path stateFilesPrefix, ID serverId) {
        PersistentLogStorage persistentLogStorage = new PersistentLogStorage(logFilePath(stateFilesPrefix));
        return new FileBasedPersistentState<>(persistentLogStorage, stateFilePath(stateFilesPrefix),
                tempSnapshotPathGenerator(stateFilesPrefix), currentSnapshotPath(stateFilesPrefix), new JavaIDSerializer<>(), serverId);
    }

    public static <ID extends Serializable> FileBasedPersistentState<ID> createOrOpen(Path stateFilesPrefix, ID serverId) throws IOException {
        Path logFilePath = logFilePath(stateFilesPrefix);
        Path stateFilePath = stateFilePath(stateFilesPrefix);
        Function<Integer, Path> tempSnapshotPathGenerator = tempSnapshotPathGenerator(stateFilesPrefix);
        deleteAnyTempSnapshots(stateFilesPrefix);
        Path currentSnapshotPath = currentSnapshotPath(stateFilesPrefix);
        if (Files.exists(logFilePath) && Files.exists(stateFilePath)) {
            PersistentLogStorage persistentLogStorage = new PersistentLogStorage(logFilePath);
            return new FileBasedPersistentState<>(persistentLogStorage, stateFilePath, tempSnapshotPathGenerator, currentSnapshotPath, new JavaIDSerializer<>());
        } else {
            Files.deleteIfExists(logFilePath);
            Files.deleteIfExists(stateFilePath);
            Files.deleteIfExists(currentSnapshotPath);
            PersistentLogStorage persistentLogStorage = new PersistentLogStorage(logFilePath);
            return new FileBasedPersistentState<>(persistentLogStorage, stateFilePath, tempSnapshotPathGenerator, currentSnapshotPath, new JavaIDSerializer<>(), serverId);
        }
    }

    private static Path currentSnapshotPath(Path stateFilesPrefix) {
        return stateFilesPrefix.resolveSibling(stateFilesPrefix.getFileName() + ".currentSnapshot");
    }

    private static void deleteAnyTempSnapshots(Path stateFilesPrefix) {
        try (final Stream<Path> pathStream = Files.find(stateFilesPrefix.getParent(), 1, (path, attr) -> Pattern.matches(stateFilesPrefix.getFileName() + "\\.snapshot\\.\\d+", path.getFileName().toString()))) {
            pathStream.forEach(FileBasedPersistentState::deleteFileOrWarn);
        } catch (IOException e) {
            LOGGER.warn("Error deleting existing snapsots", e);
        }
    }

    private static void deleteFileOrWarn(Path path) {
        try {
            Files.deleteIfExists(path);
        } catch (IOException e) {
            LOGGER.warn("Error deleting " + path, e);
        }
    }

    private static Function<Integer, Path> tempSnapshotPathGenerator(Path stateFilesPrefix) {
        return sequenceNumber -> stateFilesPrefix.resolveSibling(stateFilesPrefix.getFileName() + ".snapshot." + sequenceNumber);
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
    public FileBasedPersistentState(LogStorage logStorage, Path statePath, Function<Integer, Path> tempPathGenerator, Path currentSnapshotPath, IDSerializer<ID> idSerializer, ID id) {
        this.currentTerm = new AtomicReference<>();
        this.votedFor = new AtomicReference<>();
        this.tempSnapshotPathGenerator = tempPathGenerator;
        this.currentSnapshotPath = currentSnapshotPath;
        this.snapshotInstalledListeners = new ArrayList<>();
        try {
            this.logStorage = logStorage;
            this.fileChannel = FileChannel.open(statePath, CREATE_NEW, READ, WRITE, SYNC);
            this.idSerializer = idSerializer;
            this.id = id;
            this.currentTerm.set(new Term(0));
            writeToStateFile();
        } catch (IOException e) {
            throw new RuntimeException("Unable to create new state file", e);
        }
    }

    /**
     * Load an existing file-based persistent state
     */
    public FileBasedPersistentState(LogStorage logStorage, Path statePath, Function<Integer, Path> tempSnapshotPathGenerator, Path currentSnapshotPath, IDSerializer<ID> idSerializer) {
        this.currentTerm = new AtomicReference<>();
        this.votedFor = new AtomicReference<>();
        this.tempSnapshotPathGenerator = tempSnapshotPathGenerator;
        this.currentSnapshotPath = currentSnapshotPath;
        this.snapshotInstalledListeners = new ArrayList<>();
        try {
            this.logStorage = logStorage;
            this.fileChannel = FileChannel.open(statePath, READ, WRITE, SYNC);
            this.idSerializer = idSerializer;
            readFromStateFile();
        } catch (IOException e) {
            throw new RuntimeException("Unable to open existing state file", e);
        }
    }

    private void readFromStateFile() {
        try {
            int idLength = IOUtil.readInteger(fileChannel, START_OF_ID_LENGTH);
            currentTerm.set(new Term(IOUtil.readInteger(fileChannel, START_OF_CURRENT_TERM)));
            int votedForLength = IOUtil.readInteger(fileChannel, START_OF_VOTED_FOR_LENGTH);
            id = readIdFrom(START_OF_ID, idLength);
            votedFor.set(readIdFrom(START_OF_ID + idLength, votedForLength));
        } catch (IOException e) {
            throw new RuntimeException("Error reading from state file", e);
        }
    }

    private ID readIdFrom(long startPoint, int length) throws IOException {
        if (length == 0) {
            return null;
        }
        ByteBuffer idBuffer = ByteBuffer.allocate(length);
        fileChannel.read(idBuffer, startPoint);
        idBuffer.flip();
        return idSerializer.deserialize(idBuffer);
    }

    private void writeToStateFile() {
        try {
            ByteBuffer idBytes = idSerializer.serialize(id);
            ID votedForId = votedFor.get();
            ByteBuffer votedForBytes = votedForId != null ? idSerializer.serialize(votedForId) : ByteBuffer.allocate(0);
            IOUtil.writeInteger(fileChannel, START_OF_ID_LENGTH, idBytes.capacity());
            IOUtil.writeInteger(fileChannel, START_OF_CURRENT_TERM, currentTerm.get().getNumber());
            IOUtil.writeInteger(fileChannel, START_OF_VOTED_FOR_LENGTH, votedForBytes.capacity());
            fileChannel.write(idBytes, START_OF_ID);
            fileChannel.write(votedForBytes, START_OF_ID + idBytes.capacity());
            fileChannel.truncate(START_OF_ID + idBytes.capacity() + votedForBytes.capacity());
        } catch (IOException e) {
            throw new RuntimeException("Error writing to state file");
        }
    }

    @Override
    public ID getId() {
        return id;
    }

    @Override
    public void setCurrentTerm(Term term) {
        Term currentTerm = this.currentTerm.get();
        if (term.isGreaterThan(currentTerm)) {
            this.currentTerm.set(term);
            this.votedFor.set(null);
            writeToStateFile();
        } else if (term.isLessThan(currentTerm)) {
            throw new IllegalArgumentException("Attempted to reduce current term!");
        }
    }

    @Override
    public Term getCurrentTerm() {
        return this.currentTerm.get();
    }

    @Override
    public void setVotedFor(ID votedFor) {
        if (!Objects.equals(this.votedFor, votedFor)) {
            this.votedFor.set(votedFor);
            writeToStateFile();
        }
    }

    @Override
    public Optional<ID> getVotedFor() {
        return Optional.ofNullable(this.votedFor.get());
    }

    @Override
    public LogStorage getLogStorage() {
        return logStorage;
    }

    @Override
    public Optional<Snapshot> getCurrentSnapshot() {
        return Optional.ofNullable(currentSnapshot.get());
    }

    @Override
    public Snapshot createSnapshot(int lastIndex, Term lastTerm, ConfigurationEntry lastConfig, int snapshotOffset) {
        final Snapshot snapshot = createSnapshot(lastIndex, lastTerm, lastConfig);
        snapshot.snapshotOffset(snapshotOffset);
        return snapshot;
    }

    @Override
    public void setCurrentSnapshot(Snapshot nextSnapshot) {
        // no point installing a snapshot if we've already gone past that point
        if (nextSnapshot.getLastIndex() <= logStorage.getPrevIndex()) {
            LOGGER.warn("Not installing snapshot that would not advance us (log.prevLogIndex() == {}, nextSnapshot.getLastLogIndex() == {}",
                    logStorage.getPrevIndex(), nextSnapshot.getLastIndex());
            return;
        }
        try {
            Closeables.closeQuietly(nextSnapshot, currentSnapshot.getAndSet(null));
            Files.move(((PersistentSnapshot) nextSnapshot).path(), currentSnapshotPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            currentSnapshot.set(PersistentSnapshot.load(currentSnapshotPath));
            logStorage.installSnapshot(currentSnapshot.get());
            for (SnapshotInstalledListener listener : snapshotInstalledListeners) {
                listener.onSnapshotInstalled(currentSnapshot.get());
            }
        } catch (IOException e) {
            throw new RuntimeException("Error promoting existing snapshot");
        }
    }

    @Override
    public Snapshot createSnapshot(int lastIndex, Term lastTerm, ConfigurationEntry lastConfig) {
        return PersistentSnapshot.create(tempSnapshotPathGenerator.apply(nextSnapshotSequence.getAndIncrement()), lastIndex, lastTerm, lastConfig);
    }

    @Override
    public void addSnapshotInstalledListener(SnapshotInstalledListener listener) {
        snapshotInstalledListeners.add(listener);
    }

    @Override
    public void initialize() {
        LOGGER.warn("Initialise: prevIndex={}, lastLogIndex={}, lastLogTerm={}", logStorage.getPrevIndex(), logStorage.getLastLogIndex(), logStorage.getLastLogTerm());
        if (Files.exists(currentSnapshotPath)) {
            LOGGER.warn("Discovered snapshot, attempting to load");
            setCurrentSnapshot(PersistentSnapshot.load(currentSnapshotPath));
            LOGGER.warn("After snapshot: prevIndex={}, lastLogIndex={}, lastLogTerm={}", logStorage.getPrevIndex(), logStorage.getLastLogIndex(), logStorage.getLastLogTerm());
        }
    }
}
