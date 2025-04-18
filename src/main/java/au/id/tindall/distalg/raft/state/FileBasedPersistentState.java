package au.id.tindall.distalg.raft.state;

import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.ConfigurationEntry;
import au.id.tindall.distalg.raft.log.storage.LogStorage;
import au.id.tindall.distalg.raft.log.storage.MemoryMappedLogStorage;
import au.id.tindall.distalg.raft.log.storage.PersistentSnapshot;
import au.id.tindall.distalg.raft.serialisation.ByteBufferIO;
import au.id.tindall.distalg.raft.serialisation.IDSerializer;
import au.id.tindall.distalg.raft.serialisation.StreamingInput;
import au.id.tindall.distalg.raft.serialisation.StreamingOutput;
import au.id.tindall.distalg.raft.util.BufferUtil;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
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
public class FileBasedPersistentState<I> implements PersistentState<I>, AutoCloseable {

    private static final Logger LOGGER = getLogger();
    private static final int TRUNCATION_BUFFER = 20;
    private static final int STATE_FILE_SIZE = 1024;    // Larger than it needs to be
    public static final int STATE_WRITE_TIME_WARN_THRESHOLD_MS = 2;
    private final LogStorage logStorage;
    private final RandomAccessFile stateFile;
    private final MappedByteBuffer stateFileMap;
    private final IDSerializer idSerializer;

    private I id;
    private final AtomicReference<Term> currentTerm;
    private final AtomicReference<I> votedFor;

    private final Function<Integer, Path> tempSnapshotPathGenerator;
    private final Path currentSnapshotPath;
    private final List<SnapshotInstalledListener> snapshotInstalledListeners;
    private final AtomicReference<Snapshot> currentSnapshot = new AtomicReference<>();
    private final AtomicInteger nextSnapshotSequence = new AtomicInteger(0);


    public static <I> FileBasedPersistentState<I> create(IDSerializer idSerializer, Path stateDirectory, I serverId) {
        MemoryMappedLogStorage persistentLogStorage = new MemoryMappedLogStorage(idSerializer, logFilePath(stateDirectory), TRUNCATION_BUFFER);
        return new FileBasedPersistentState<>(persistentLogStorage, stateFilePath(stateDirectory),
                tempSnapshotPathGenerator(stateDirectory), currentSnapshotPath(stateDirectory), idSerializer, serverId);
    }

    public static <I> FileBasedPersistentState<I> createOrOpen(IDSerializer idSerializer, Path stateDirectory, I serverId) throws IOException {
        Path logFilePath = logFilePath(stateDirectory);
        Path stateFilePath = stateFilePath(stateDirectory);
        Function<Integer, Path> tempSnapshotPathGenerator = tempSnapshotPathGenerator(stateDirectory);
        deleteAnyTempSnapshots(stateDirectory);
        Path currentSnapshotPath = currentSnapshotPath(stateDirectory);
        if (Files.exists(logFilePath) && Files.exists(stateFilePath)) {
            MemoryMappedLogStorage persistentLogStorage = new MemoryMappedLogStorage(idSerializer, logFilePath, TRUNCATION_BUFFER);
            return new FileBasedPersistentState<>(persistentLogStorage, stateFilePath, tempSnapshotPathGenerator, currentSnapshotPath, idSerializer);
        } else {
            Files.deleteIfExists(logFilePath);
            Files.deleteIfExists(stateFilePath);
            Files.deleteIfExists(currentSnapshotPath);
            MemoryMappedLogStorage persistentLogStorage = new MemoryMappedLogStorage(idSerializer, logFilePath, TRUNCATION_BUFFER);
            return new FileBasedPersistentState<>(persistentLogStorage, stateFilePath, tempSnapshotPathGenerator, currentSnapshotPath, idSerializer, serverId);
        }
    }

    private static Path currentSnapshotPath(Path stateFilesPrefix) {
        return stateFilesPrefix.resolve("currentSnapshot");
    }

    private static void deleteAnyTempSnapshots(Path stateFilesDirectory) {
        deleteFilesMatching(stateFilesDirectory, 1,
                (path, attr) -> Pattern.matches("snapshot\\.\\d+", path.getFileName().toString()));
    }

    private static Function<Integer, Path> tempSnapshotPathGenerator(Path stateFilesDirectory) {
        return sequenceNumber -> stateFilesDirectory.resolve("snapshot." + sequenceNumber);
    }

    private static Path stateFilePath(Path stateFilesPrefix) {
        return stateFilesPrefix.resolve("state");
    }

    private static Path logFilePath(Path stateFilesPrefix) {
        return stateFilesPrefix.resolve("log");
    }

    /**
     * Create a new file-based persistent state
     */
    public FileBasedPersistentState(LogStorage logStorage, Path statePath, Function<Integer, Path> tempPathGenerator, Path currentSnapshotPath, IDSerializer idSerializer, I id) {
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
            throw new UncheckedIOException("Unable to create new state file", e);
        }
    }

    /**
     * Load an existing file-based persistent state
     */
    public FileBasedPersistentState(LogStorage logStorage, Path statePath, Function<Integer, Path> tempSnapshotPathGenerator, Path currentSnapshotPath, IDSerializer idSerializer) {
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
            throw new UncheckedIOException("Unable to open existing state file", e);
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
        final ByteBufferIO bbio = ByteBufferIO.wrap(idSerializer, stateFileMap);
        id = bbio.readIdentifier();
        currentTerm.set(bbio.readStreamable());
        votedFor.set(bbio.readNullable(StreamingInput::readIdentifier));
    }

    private void writeToStateFile() {
        final long startTime = System.currentTimeMillis();
        final ByteBufferIO bbio = ByteBufferIO.wrap(idSerializer, stateFileMap);
        bbio.setWritePosition(0);
        bbio.writeIdentifier(id);
        bbio.writeStreamable(currentTerm.get());
        bbio.writeNullable(votedFor.get(), StreamingOutput::writeIdentifier);
        long duration = System.currentTimeMillis() - startTime;
        if (duration > STATE_WRITE_TIME_WARN_THRESHOLD_MS) {
            LOGGER.warn("Took {}ms to write state file (expected < {})", duration, STATE_WRITE_TIME_WARN_THRESHOLD_MS);
        }
    }

    @Override
    public I getId() {
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
    public void setVotedFor(I votedFor) {
        if (!Objects.equals(this.votedFor.get(), votedFor)) {
            this.votedFor.set(votedFor);
            writeToStateFile();
        }
    }

    @Override
    public void setCurrentTermAndVotedFor(Term term, I votedFor) {
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
    public Optional<I> getVotedFor() {
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
            currentSnapshot.set(PersistentSnapshot.load(idSerializer, currentSnapshotPath));
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
        return PersistentSnapshot.create(idSerializer, tempSnapshotPathGenerator.apply(nextSnapshotSequence.getAndIncrement()), lastIndex, lastTerm, lastConfig);
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
                setCurrentSnapshot(PersistentSnapshot.load(idSerializer, currentSnapshotPath));
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
        BufferUtil.free(stateFileMap);
    }
}