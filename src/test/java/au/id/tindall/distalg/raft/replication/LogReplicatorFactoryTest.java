package au.id.tindall.distalg.raft.replication;

import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.state.PersistentState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class LogReplicatorFactoryTest {

    private static final int MAX_BATCH_SIZE = 1234;
    private static final long FOLLOWER_ID = 9876L;
    private static final Term CURRENT_TERM = new Term(1);
    private static final int NEXT_LOG_INDEX = 543;

    @Mock
    private Log log;
    @Mock
    private Cluster<Long> cluster;
    @Mock
    private PersistentState<Long> persistentState;

    private LogReplicatorFactory<Long> logReplicatorFactory;

    @BeforeEach
    void setUp() {
        when(persistentState.getCurrentTerm()).thenReturn(CURRENT_TERM);
        when(log.getNextLogIndex()).thenReturn(NEXT_LOG_INDEX);
        logReplicatorFactory = new LogReplicatorFactory<>(log, persistentState, cluster, MAX_BATCH_SIZE);
    }

    @Test
    void willCreateLogReplicator() {
        assertThat(logReplicatorFactory.createLogReplicator(FOLLOWER_ID))
                .usingRecursiveComparison()
                .isEqualTo(new LogReplicator<>(log, CURRENT_TERM, cluster, FOLLOWER_ID, MAX_BATCH_SIZE, NEXT_LOG_INDEX));
    }
}