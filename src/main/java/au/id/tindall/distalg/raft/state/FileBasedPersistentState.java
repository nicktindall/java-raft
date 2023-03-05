package au.id.tindall.distalg.raft.state;

import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.ConfigurationEntry;
import au.id.tindall.distalg.raft.log.storage.LogStorage;
import au.id.tindall.distalg.raft.log.storage.PersistentLogStorage;
import au.id.tindall.distalg.raft.log.storage.PersistentSnapshot;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.regex.Pattern;

import static au.id.tindall.distalg.raft.util.Closeables.closeQuietly;
import static au.id.tindall.distalg.raft.util.FileUtil.deleteFilesMatching;
import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
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
public class FileBasedPersistentState<ID extends Serializable> implements PersistentState<ID>, AutoCloseable {

    private static final Logger LOGGER = getLogger();
    private static final int TRUNCATION_BUFFER = 20;
    private static final int START_OF_ID_LENGTH = 0;
    private static final int START_OF_CURRENT_TERM = 4;
    private static final int START_OF_VOTED_FOR_LENGTH = 8;
    private static final int START_OF_ID = 12;
    private static final int STATE_FILE_SIZE = 1024;    // Larger than it needs to be
    public static final int STATE_WRITE_TIME_WARN_THRESHOLD_MS = 2;
    private final LogStorage logStorage;
    private final RandomAccessFile stateFile;
    private final MappedByteBuffer stateFileMap;
    private final IDSerializer<ID> idSerializer;

    private ID id;
    private final AtomicReference<Term> currentTerm;
    private final AtomicReference<ID> votedFor;

    private final Function<Integer, Path> tempSnapshotPathGenerator;
    private final Path currentSnapshotPath;
    private final List<SnapshotInstalledListener> snapshotInstalledListeners;
    private final AtomicReference<Snapshot> currentSnapshot = new AtomicReference<>();
    private final AtomicInteger nextSnapshotSequence = new AtomicInteger(0);


    public static <ID extends Serializable> FileBasedPersistentState<ID> create(Path stateFilesPrefix, ID serverId) {
        PersistentLogStorage persistentLogStorage = new PersistentLogStorage(logFilePath(stateFilesPrefix), TRUNCATION_BUFFER);
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
            PersistentLogStorage persistentLogStorage = new PersistentLogStorage(logFilePath, TRUNCATION_BUFFER);
            return new FileBasedPersistentState<>(persistentLogStorage, stateFilePath, tempSnapshotPathGenerator, currentSnapshotPath, new JavaIDSerializer<>());
        } else {
            Files.deleteIfExists(logFilePath);
            Files.deleteIfExists(stateFilePath);
            Files.deleteIfExists(currentSnapshotPath);
            PersistentLogStorage persistentLogStorage = new PersistentLogStorage(logFilePath, TRUNCATION_BUFFER);
            return new FileBasedPersistentState<>(persistentLogStorage, stateFilePath, tempSnapshotPathGenerator, currentSnapshotPath, new JavaIDSerializer<>(), serverId);
        }
    }

    private static Path currentSnapshotPath(Path stateFilesPrefix) {
        return stateFilesPrefix.resolveSibling(stateFilesPrefix.getFileName() + ".currentSnapshot");
    }

    private static void deleteAnyTempSnapshots(Path stateFilesPrefix) {
        deleteFilesMatching(stateFilesPrefix.getParent(), 1,
                (path, attr) -> Pattern.matches(stateFilesPrefix.getFileName() + "\\.snapshot\\.\\d+", path.getFileName().toString()));
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
            this.stateFile = initialiseStateFile(statePath);
            this.stateFileMap = mapStateFile(stateFile);
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
            this.stateFile = initialiseStateFile(statePath);
            this.stateFileMap = mapStateFile(stateFile);
            this.idSerializer = idSerializer;
            readFromStateFile();
        } catch (IOException e) {
            throw new RuntimeException("Unable to open existing state file", e);
        }
    }

    private RandomAccessFile initialiseStateFile(Path statePath) throws IOException {
        RandomAccessFile sf = new RandomAccessFile(statePath.toFile(), "rw");
        if (sf.length() < STATE_FILE_SIZE) {
            sf.setLength(STATE_FILE_SIZE);
        }
        return sf;
    }

    private MappedByteBuffer mapStateFile(RandomAccessFile randomAccessFile) throws IOException {
        return randomAccessFile.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, STATE_FILE_SIZE);
    }

    private void readFromStateFile() {
        try {
            int idLength = stateFileMap.getInt(START_OF_ID_LENGTH);
            currentTerm.set(new Term(stateFileMap.getInt(START_OF_CURRENT_TERM)));
            int votedForLength = stateFileMap.getInt(START_OF_VOTED_FOR_LENGTH);
            id = readIdFrom(START_OF_ID, idLength);
            votedFor.set(readIdFrom(START_OF_ID + idLength, votedForLength));
        } catch (IOException e) {
            throw new RuntimeException("Error reading from state file", e);
        }
    }

    private ID readIdFrom(int startPoint, int length) throws IOException {
        if (length == 0) {
            return null;
        }
        ByteBuffer buffer = ByteBuffer.allocate(length);
        stateFileMap.position(startPoint);
        stateFileMap.get(buffer.array());
        return idSerializer.deserialize(buffer);
    }

    private void writeToStateFile() {
        long startTime = System.currentTimeMillis();
        ByteBuffer idBytes = idSerializer.serialize(id);
        ID votedForId = votedFor.get();
        ByteBuffer votedForBytes = votedForId != null ? idSerializer.serialize(votedForId) : ByteBuffer.allocate(0);
        stateFileMap.putInt(START_OF_ID_LENGTH, idBytes.capacity());
        stateFileMap.putInt(START_OF_CURRENT_TERM, currentTerm.get().getNumber());
        stateFileMap.putInt(START_OF_VOTED_FOR_LENGTH, votedForBytes.capacity());
        stateFileMap.position(START_OF_ID).put(idBytes);
        stateFileMap.put(votedForBytes);
        long startOfForce = System.currentTimeMillis();
        stateFileMap.force();
        long forceDuration = System.currentTimeMillis() - startOfForce;
        long duration = System.currentTimeMillis() - startTime;
        if (duration > STATE_WRITE_TIME_WARN_THRESHOLD_MS) {
            LOGGER.warn("Took {}ms to write state file (expected < {}, forceDuration={})", duration, STATE_WRITE_TIME_WARN_THRESHOLD_MS, forceDuration);
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
        if (!Objects.equals(this.votedFor.get(), votedFor)) {
            this.votedFor.set(votedFor);
            writeToStateFile();
        }
    }

    @Override
    public void setCurrentTermAndVotedFor(Term term, ID votedFor) {
        Term currentTerm = this.currentTerm.get();
        if (term.isLessThan(currentTerm)) {
            throw new IllegalArgumentException("Attempted to reduce current term!");
        }
        if (term.isGreaterThan(currentTerm) || !Objects.equals(this.votedFor.get(), votedFor)) {
            this.currentTerm.set(term);
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
    public Snapshot createSnapshot(int lastIndex, Term lastTerm, ConfigurationEntry lastConfig, int snapshotOffset) throws IOException {
        final Snapshot snapshot = createSnapshot(lastIndex, lastTerm, lastConfig);
        snapshot.setSnapshotOffset(snapshotOffset);
        return snapshot;
    }

    @Override
    public void setCurrentSnapshot(Snapshot nextSnapshot) throws IOException {
        if (snapshotIsNotLaterThanCurrentSnapshot(nextSnapshot)) {
            LOGGER.debug("Not installing snapshot that would not advance us (currentSnapshot.getLastIndex() == {}, nextSnapshot.getLastLogIndex() == {}",
                    currentSnapshot.get().getLastIndex(), nextSnapshot.getLastIndex());
            return;
        }
        try {
            closeQuietly(nextSnapshot, currentSnapshot.getAndSet(null));
            if (!(Files.exists(currentSnapshotPath) && Files.isSameFile(((PersistentSnapshot) nextSnapshot).path(), currentSnapshotPath))) {
                Files.move(((PersistentSnapshot) nextSnapshot).path(), currentSnapshotPath, ATOMIC_MOVE, REPLACE_EXISTING);
                if (Files.exists(((PersistentSnapshot) nextSnapshot).path())) {
                    LOGGER.warn("Couldn't delete temporary snapshot");
                }
            }
            currentSnapshot.set(PersistentSnapshot.load(currentSnapshotPath));
            logStorage.installSnapshot(currentSnapshot.get());
            for (SnapshotInstalledListener listener : snapshotInstalledListeners) {
                listener.onSnapshotInstalled(currentSnapshot.get());
            }
        } catch (IOException e) {
            throw new IOException("Error promoting existing snapshot", e);
        }
    }

    private boolean snapshotIsNotLaterThanCurrentSnapshot(Snapshot newSnapshot) {
        // no point installing a snapshot if we've already gone past that point
        final Snapshot previousSnapshot = currentSnapshot.get();
        return previousSnapshot != null && newSnapshot.getLastIndex() <= previousSnapshot.getLastIndex();
    }

    @Override
    public Snapshot createSnapshot(int lastIndex, Term lastTerm, ConfigurationEntry lastConfig) throws IOException {
        return PersistentSnapshot.create(tempSnapshotPathGenerator.apply(nextSnapshotSequence.getAndIncrement()), lastIndex, lastTerm, lastConfig);
    }

    @Override
    public void addSnapshotInstalledListener(SnapshotInstalledListener listener) {
        snapshotInstalledListeners.add(listener);
    }

    @Override
    public void initialize() {
        LOGGER.debug("Initialise: prevIndex={}, lastLogIndex={}, lastLogTerm={}", logStorage.getPrevIndex(), logStorage.getLastLogIndex(), logStorage.getLastLogTerm());
        if (Files.exists(currentSnapshotPath)) {
            LOGGER.debug("Discovered snapshot, attempting to load");
            try {
                setCurrentSnapshot(PersistentSnapshot.load(currentSnapshotPath));
                LOGGER.debug("After snapshot: prevIndex={}, lastLogIndex={}, lastLogTerm={}", logStorage.getPrevIndex(), logStorage.getLastLogIndex(), logStorage.getLastLogTerm());
            } catch (IOException e) {
                LOGGER.error("Failed to load snapshot", e);
            }
        } else if (logStorage.getPrevIndex() > 0) {
            LOGGER.error("prevIndex > 0 (={}), but no current snapshot could be found (currentSnapshotPath={})", logStorage.getPrevIndex(), currentSnapshotPath);
        }
    }

    @Override
    public void close() {
        closeQuietly(logStorage, stateFileMap, stateFile);
    }
}