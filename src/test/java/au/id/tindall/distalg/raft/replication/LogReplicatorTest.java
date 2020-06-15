package au.id.tindall.distalg.raft.replication;

import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.LogEntry;
import au.id.tindall.distalg.raft.log.entries.StateMachineCommandEntry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;

import static au.id.tindall.distalg.raft.DomainUtils.logContaining;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
public class LogReplicatorTest {

    private static final long FOLLOWER_ID = 456L;
    private static final int INITIAL_NEXT_INDEX = 5;
    private static final Term CURRENT_TERM = new Term(3);
    private static final int COMMIT_INDEX = 3;
    private static final int LAST_LOG_INDEX = 4;
    private static final int CLIENT_ID = 123;
    private static final LogEntry ENTRY_ONE = new StateMachineCommandEntry(new Term(0), CLIENT_ID, 0, "one".getBytes());
    private static final LogEntry ENTRY_TWO = new StateMachineCommandEntry(new Term(1), CLIENT_ID, 1, "two".getBytes());
    private static final LogEntry ENTRY_THREE = new StateMachineCommandEntry(new Term(2), CLIENT_ID, 2, "three".getBytes());
    private static final LogEntry ENTRY_FOUR = new StateMachineCommandEntry(new Term(2), CLIENT_ID, 3, "four".getBytes());

    private LogReplicator<Long> logReplicator;

    @Mock
    private Cluster<Long> cluster;

    private Log log;

    @BeforeEach
    public void setUp() {
        log = logContaining(ENTRY_ONE, ENTRY_TWO, ENTRY_THREE, ENTRY_FOUR);
        log.advanceCommitIndex(COMMIT_INDEX);
        logReplicator = new LogReplicator<>(cluster, FOLLOWER_ID, INITIAL_NEXT_INDEX);
    }

    @Test
    public void matchIndexWillBeInitializedToZero() {
        assertThat(logReplicator.getMatchIndex()).isZero();
    }

    @Test
    public void nextIndexWillBeInitializedToValuePassed() {
        assertThat(logReplicator.getNextIndex()).isEqualTo(INITIAL_NEXT_INDEX);
    }

    @Test
    public void logSuccessResponseWillSetNextIndex() {
        int lastAppendedIndex = 2;
        logReplicator.logSuccessResponse(lastAppendedIndex);
        assertThat(logReplicator.getNextIndex()).isEqualTo(lastAppendedIndex + 1);
    }

    @Test
    public void logSuccessResponseWillSetMatchIndex() {
        int lastAppendedIndex = 2;
        logReplicator.logSuccessResponse(lastAppendedIndex);
        assertThat(logReplicator.getMatchIndex()).isEqualTo(lastAppendedIndex);
    }

    @Test
    public void logFailedResponseWillDecrementNextIndex() {
        logReplicator.logFailedResponse();
        assertThat(logReplicator.getNextIndex()).isEqualTo(INITIAL_NEXT_INDEX - 1);
    }

    @Test
    public void logFailedResponseWillNotModifyMatchIndex() {
        logReplicator.logFailedResponse();
        assertThat(logReplicator.getMatchIndex()).isZero();
    }

    @Test
    public void shouldSendEmptyAppendEntriesRequest_WhenThereAreNoLogEntries() {
        logReplicator = new LogReplicator<>(cluster, FOLLOWER_ID, 1);
        logReplicator.sendAppendEntriesRequest(CURRENT_TERM, logContaining());
        verify(cluster).sendAppendEntriesRequest(CURRENT_TERM, FOLLOWER_ID, 0,
                Optional.empty(), emptyList(), 0);
    }

    @Test
    public void shouldSendEmptyAppendEntriesRequest_WhenFollowerIsCaughtUp() {
        logReplicator.sendAppendEntriesRequest(CURRENT_TERM, log);
        verify(cluster).sendAppendEntriesRequest(CURRENT_TERM, FOLLOWER_ID, LAST_LOG_INDEX,
                Optional.of(ENTRY_FOUR.getTerm()), emptyList(), COMMIT_INDEX);
    }

    @Test
    public void shouldSendSingleEntryAppendEntriesRequest_WhenFollowerIsLagging() {
        logReplicator = new LogReplicator<>(cluster, FOLLOWER_ID, LAST_LOG_INDEX - 1);
        logReplicator.sendAppendEntriesRequest(CURRENT_TERM, log);
        verify(cluster).sendAppendEntriesRequest(CURRENT_TERM, FOLLOWER_ID, LAST_LOG_INDEX - 2,
                Optional.of(ENTRY_TWO.getTerm()), singletonList(ENTRY_THREE), COMMIT_INDEX);
    }
}