package au.id.tindall.distalg.raft.snapshotting;

import au.id.tindall.distalg.raft.client.sessions.ClientSessionStore;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.ClientRegistrationEntry;
import au.id.tindall.distalg.raft.log.entries.ConfigurationEntry;
import au.id.tindall.distalg.raft.log.storage.LogStorage;
import au.id.tindall.distalg.raft.state.InMemoryPersistentState;
import au.id.tindall.distalg.raft.state.Snapshot;
import au.id.tindall.distalg.raft.statemachine.StateMachine;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.ByteBuffer;
import java.util.Set;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SnapshotterTest {

    private static final Term TERM_ONE = new Term(1);
    private static final ConfigurationEntry LAST_CONFIGURATION_ENTRY = new ConfigurationEntry(TERM_ONE, Set.of(45, 67));
    private static final Term TERM_TWO = new Term(2);
    private static final int SERVER_ID = 123;
    private static final byte[] SNAPSHOT_BYTES = "SNAPSHOT_BYTES".getBytes();
    private static final byte[] SESSIONS_BYTES = "SESSIONS_BYTES".getBytes();
    @Mock
    private StateMachine stateMachine;
    @Mock
    private SnapshotHeuristic snapshotHeuristic;
    @Mock
    private ClientSessionStore clientSessionStore;
    private Log log;
    private InMemoryPersistentState<Integer> inMemoryPersistentState;

    private Snapshotter snapshotter;

    @BeforeEach
    void setUp() {
        inMemoryPersistentState = new InMemoryPersistentState<>(SERVER_ID);
        LogStorage logStorage = inMemoryPersistentState.getLogStorage();
        IntStream.range(1, 21).forEach(i -> {
            logStorage.add(i, new ClientRegistrationEntry(i < 11 ? TERM_ONE : TERM_TWO, i));
        });
        log = new Log(logStorage);
        snapshotter = new Snapshotter(log, clientSessionStore, stateMachine, inMemoryPersistentState, snapshotHeuristic);
        lenient().when(stateMachine.createSnapshot()).thenReturn(SNAPSHOT_BYTES);
        lenient().when(clientSessionStore.serializeSessions()).thenReturn(SESSIONS_BYTES);
    }

    @Test
    void willCreateSnapshotWhenDue() {
        when(snapshotHeuristic.shouldCreateSnapshot(log, stateMachine, null)).thenReturn(true);
        snapshotter.logConfigurationEntry(LAST_CONFIGURATION_ENTRY);
        snapshotter.createSnapshotIfReady(5);
        assertThat(inMemoryPersistentState.getCurrentSnapshot()).isNotEmpty();
        final Snapshot currentSnapshot = inMemoryPersistentState.getCurrentSnapshot().get();
        assertThat(currentSnapshot.getLastIndex()).isEqualTo(5);
        assertThat(currentSnapshot.getLastTerm()).isEqualTo(TERM_ONE);
        ByteBuffer snapshotContents = ByteBuffer.allocate(SNAPSHOT_BYTES.length);
        currentSnapshot.readInto(snapshotContents, currentSnapshot.getSnapshotOffset());
        assertThat(snapshotContents.array()).isEqualTo(SNAPSHOT_BYTES);
        ByteBuffer sessionsContents = ByteBuffer.allocate(SESSIONS_BYTES.length);
        currentSnapshot.readInto(sessionsContents, 0);
        assertThat(sessionsContents.array()).isEqualTo(SESSIONS_BYTES);
        assertThat(currentSnapshot.getLastConfig()).usingRecursiveComparison().isEqualTo(LAST_CONFIGURATION_ENTRY);
    }

    @Test
    void willNotCreateSnapshotWhenNotDue() {
        when(snapshotHeuristic.shouldCreateSnapshot(log, stateMachine, null)).thenReturn(false);
        snapshotter.createSnapshotIfReady(5);
        assertThat(inMemoryPersistentState.getCurrentSnapshot()).isEmpty();
    }

    @Test
    void willNotCreateSnapshotWhenErrorOccursDuringCreation() {
        when(snapshotHeuristic.shouldCreateSnapshot(log, stateMachine, null)).thenReturn(true);
        when(clientSessionStore.serializeSessions()).thenThrow(new IllegalStateException("Error serializing sessions"));
        snapshotter.createSnapshotIfReady(5);
        assertThat(inMemoryPersistentState.getCurrentSnapshot()).isEmpty();
    }
}