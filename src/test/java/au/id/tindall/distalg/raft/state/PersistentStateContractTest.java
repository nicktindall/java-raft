package au.id.tindall.distalg.raft.state;

import au.id.tindall.distalg.raft.log.LogSummary;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.ConfigurationEntry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

abstract class PersistentStateContractTest {

    protected static final int SERVER_ID = 123;
    protected static final int OTHER_SERVER_ID = 456;
    protected static final int LAST_INDEX = 1234;
    protected static final Term LAST_TERM = new Term(10);
    protected static final ConfigurationEntry LAST_CONFIG = new ConfigurationEntry(new Term(3), Set.of(1, 2, 3));
    protected static final byte[] SESSION_STATE_BYTES = "session_state".getBytes();
    protected static final byte[] SNAPSHOT_BYTES = "snapshot_bytes".getBytes();

    protected PersistentState<Integer> persistentState;

    @BeforeEach
    void setUp() {
        persistentState = createPersistentState();
    }

    protected abstract PersistentState<Integer> createPersistentState();

    @Test
    void willReturnServerId() {
        assertThat(persistentState.getId()).isEqualTo(SERVER_ID);
    }

    @Nested
    class SetCurrentTerm {

        @Test
        void willSetCurrentTerm() {
            final Term term = LAST_TERM;
            persistentState.setCurrentTerm(term);
            assertThat(persistentState.getCurrentTerm()).isEqualTo(term);
        }

        @Test
        void willThrow_WhenTermIsLessThanCurrentTerm() {
            persistentState.setCurrentTerm(LAST_TERM);
            final Term termSix = new Term(6);
            assertThatThrownBy(() -> persistentState.setCurrentTerm(termSix))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void willClearVotedFor_WhenTermIsAdvanced() {
            persistentState.setVotedFor(OTHER_SERVER_ID);
            persistentState.setCurrentTerm(LAST_TERM);
            assertThat(persistentState.getVotedFor()).isEmpty();
        }

        @Test
        void willNotClearVotedFor_WhenTermIsNotAdvanced() {
            persistentState.setCurrentTerm(LAST_TERM);
            persistentState.setVotedFor(OTHER_SERVER_ID);
            persistentState.setCurrentTerm(LAST_TERM);
            assertThat(persistentState.getVotedFor()).contains(OTHER_SERVER_ID);
        }
    }

    @Nested
    class SetCurrentSnapshot {

        private Snapshot createdSnapshot;

        @BeforeEach
        void setUp() throws IOException {
            createdSnapshot = createSnapshot(12345, new Term(102));
        }

        private Snapshot createSnapshot(int lastIndex, Term lastTerm) throws IOException {
            Snapshot snapshot = persistentState.createSnapshot(lastIndex, lastTerm, new ConfigurationEntry(LAST_TERM, Set.of(1, 2, 3)));
            snapshot.writeBytes(0, "sessions_bytes".getBytes());
            snapshot.finaliseSessions();
            snapshot.writeBytes(snapshot.getSnapshotOffset(), SNAPSHOT_BYTES);
            snapshot.finalise();
            return snapshot;
        }

        @Test
        void willSetCurrentSnapshot() throws IOException {
            ByteBuffer originalBytes = ByteBuffer.allocate((int) createdSnapshot.getLength());
            createdSnapshot.readInto(originalBytes, 0);
            persistentState.setCurrentSnapshot(createdSnapshot);
            assertThat(persistentState.getCurrentSnapshot()).isNotEmpty();
            final Snapshot currentSnapshot = persistentState.getCurrentSnapshot().get();
            assertThat(currentSnapshot.getLastConfig()).usingRecursiveComparison()
                    .isEqualTo(createdSnapshot.getLastConfig());
            assertThat(currentSnapshot.getSnapshotOffset()).isEqualTo(createdSnapshot.getSnapshotOffset());
            assertThat(currentSnapshot.getLastIndex()).isEqualTo(createdSnapshot.getLastIndex());
            assertThat(currentSnapshot.getLastTerm()).isEqualTo(createdSnapshot.getLastTerm());
            assertThat(currentSnapshot.getLength()).isEqualTo(createdSnapshot.getLength());
            ByteBuffer restoredBytes = ByteBuffer.allocate((int) currentSnapshot.getLength());
            currentSnapshot.readInto(restoredBytes, 0);
            assertThat(originalBytes).isEqualTo(restoredBytes);
        }

        @Test
        void willNotifyListeners() throws IOException {
            AtomicReference<LogSummary> installed = new AtomicReference<>();
            persistentState.addSnapshotInstalledListener(snapshot -> {
                installed.set(new LogSummary(Optional.of(snapshot.getLastTerm()), snapshot.getLastIndex()));
            });
            persistentState.setCurrentSnapshot(createdSnapshot);
            assertThat(installed).hasValue(new LogSummary(Optional.of(createdSnapshot.getLastTerm()), createdSnapshot.getLastIndex()));
        }

        @Test
        void willNotInstallSnapshotEarlierThanItsCurrentSnapshot() throws IOException {
            persistentState.setCurrentSnapshot(createSnapshot(123, new Term(5)));
            assertThat(persistentState.getCurrentSnapshot().get().getLastIndex()).isEqualTo(123);
            persistentState.setCurrentSnapshot(createSnapshot(45, new Term(3)));
            assertThat(persistentState.getCurrentSnapshot().get().getLastIndex()).isEqualTo(123);
        }
    }

    @Nested
    class CreateSnapshot {

        @Test
        void willCreateForNewSnapshot() throws IOException {
            final Snapshot snapshot = persistentState.createSnapshot(LAST_INDEX, LAST_TERM, LAST_CONFIG);
            snapshot.writeBytes(0, SESSION_STATE_BYTES);
            snapshot.finaliseSessions();
            snapshot.writeBytes(snapshot.getSnapshotOffset(), SNAPSHOT_BYTES);
            snapshot.finalise();
            assertThat(snapshot.getLastIndex()).isEqualTo(LAST_INDEX);
            assertThat(snapshot.getLastTerm()).isEqualTo(LAST_TERM);
            assertThat(snapshot.getLastConfig()).usingRecursiveComparison().isEqualTo(LAST_CONFIG);
            assertThat(getSnapshotBytes(snapshot)).isEqualTo(ByteBuffer.wrap(SNAPSHOT_BYTES));
            assertThat(getSessionsBytes(snapshot)).isEqualTo(ByteBuffer.wrap(SESSION_STATE_BYTES));
        }

        @Test
        void willCreateForReceivedSnapshot() throws IOException {
            final Snapshot snapshot = persistentState.createSnapshot(LAST_INDEX, LAST_TERM, LAST_CONFIG, SESSION_STATE_BYTES.length);
            snapshot.writeBytes(0, SESSION_STATE_BYTES);
            snapshot.writeBytes(SESSION_STATE_BYTES.length, SNAPSHOT_BYTES);
            snapshot.finalise();
            assertThat(snapshot.getLastIndex()).isEqualTo(LAST_INDEX);
            assertThat(snapshot.getLastTerm()).isEqualTo(LAST_TERM);
            assertThat(snapshot.getLastConfig()).usingRecursiveComparison().isEqualTo(LAST_CONFIG);
            assertThat(getSnapshotBytes(snapshot)).isEqualTo(ByteBuffer.wrap(SNAPSHOT_BYTES));
            assertThat(getSessionsBytes(snapshot)).isEqualTo(ByteBuffer.wrap(SESSION_STATE_BYTES));
        }
    }

    private ByteBuffer getSnapshotBytes(Snapshot snapshot) {
        ByteBuffer buffer = ByteBuffer.allocate((int) snapshot.getLength() - snapshot.getSnapshotOffset());
        snapshot.readInto(buffer, snapshot.getSnapshotOffset());
        buffer.flip();
        return buffer;
    }

    private ByteBuffer getSessionsBytes(Snapshot snapshot) {
        ByteBuffer buffer = ByteBuffer.allocate(snapshot.getSnapshotOffset());
        snapshot.readInto(buffer, 0);
        buffer.flip();
        return buffer;
    }
}