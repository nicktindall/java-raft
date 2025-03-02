package au.id.tindall.distalg.raft.state;

import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.ClientRegistrationEntry;
import au.id.tindall.distalg.raft.log.entries.ConfigurationEntry;
import au.id.tindall.distalg.raft.log.storage.LogStorage;
import au.id.tindall.distalg.raft.log.storage.MemoryMappedLogStorage;
import au.id.tindall.distalg.raft.serialisation.IntegerIDSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MockitoExtension.class)
class FileBasedPersistentStateTest extends PersistentStateContractTest {

    private static final Term CURRENT_TERM = new Term(9);
    @TempDir
    Path tempDir;
    private Path stateFile;
    private Function<Integer, Path> nextSnapshotPath;
    private int nextSnapshotSequence = 0;
    private Path currentSnapshotPath;
    @Mock
    private LogStorage logStorage;

    @BeforeEach
    void setUp() {
        nextSnapshotSequence = 0;
        stateFile = tempDir.resolve("stateFile");
        nextSnapshotPath = sequence -> tempDir.resolve("nextSnapshot" + nextSnapshotSequence++);
        currentSnapshotPath = tempDir.resolve("currentSnapshot");
        super.setUp();
    }

    @Override
    protected PersistentState<Integer> createPersistentState() {
        return new FileBasedPersistentState<>(logStorage, stateFile, nextSnapshotPath, currentSnapshotPath, IntegerIDSerializer.INSTANCE, PersistentStateContractTest.SERVER_ID);
    }

    @Test
    void willRestoreFromFileAndLoadSnapshotOnInitialise() throws IOException {
        persistentState.setCurrentTerm(CURRENT_TERM);
        persistentState.setVotedFor(OTHER_SERVER_ID);
        final Snapshot snapshot = persistentState.createSnapshot(1234, new Term(10), new ConfigurationEntry(new Term(5), Set.of(1, 2)), 10);
        snapshot.writeBytes(0, "Whole snapshot bytes".getBytes());
        persistentState.setCurrentSnapshot(snapshot);
        PersistentState<Integer> restored = new FileBasedPersistentState<>(logStorage, stateFile, nextSnapshotPath, currentSnapshotPath, IntegerIDSerializer.INSTANCE);
        assertThat(restored.getId()).isEqualTo(SERVER_ID);
        assertThat(restored.getCurrentTerm()).isEqualTo(CURRENT_TERM);
        assertThat(restored.getVotedFor()).contains(OTHER_SERVER_ID);
        restored.initialize();
        assertThat(restored.getCurrentSnapshot().get().getLastIndex()).isEqualTo(snapshot.getLastIndex());
    }

    @Test
    void willCreateStateAndLogStorage() throws IOException {
        final Path otherStateFile = tempDir.resolve("otherStateFile");
        Files.createDirectories(otherStateFile);
        final FileBasedPersistentState<Integer> ps = FileBasedPersistentState.create(IntegerIDSerializer.INSTANCE, otherStateFile, SERVER_ID);
        assertThat(ps.getLogStorage()).isInstanceOf(MemoryMappedLogStorage.class);
    }

    @Test
    void willCreateOrOpenStateAndLogStorage() throws IOException {
        final Path otherStateFile = tempDir.resolve("otherStateFile");
        Files.createDirectories(otherStateFile);
        final ClientRegistrationEntry logEntry = new ClientRegistrationEntry(Term.ZERO, 678);
        final FileBasedPersistentState<Integer> ps = FileBasedPersistentState.createOrOpen(IntegerIDSerializer.INSTANCE, otherStateFile, SERVER_ID);
        ps.getLogStorage().add(1, logEntry);

        final FileBasedPersistentState<Integer> restoredPs = FileBasedPersistentState.createOrOpen(IntegerIDSerializer.INSTANCE, otherStateFile, SERVER_ID);
        assertThat(restoredPs.getLogStorage().getEntry(1)).usingRecursiveComparison().isEqualTo(logEntry);
    }
}