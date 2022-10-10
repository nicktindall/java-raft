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
    private static final Term CURRENT_TERM = new Term(1);

    @Mock
    private Log log;
    @Mock
    private Cluster<Long> cluster;
    @Mock
    private PersistentState<Long> persistentState;
    @Mock
    private ReplicationState<Long> replicationState;

    private LogReplicatorFactory<Long> logReplicatorFactory;

    @BeforeEach
    void setUp() {
        when(persistentState.getCurrentTerm()).thenReturn(CURRENT_TERM);
        logReplicatorFactory = new LogReplicatorFactory<>(log, persistentState, cluster, MAX_BATCH_SIZE);
    }

    @Test
    void willCreateLogReplicator() {
        assertThat(logReplicatorFactory.createLogReplicator(replicationState))
                .usingRecursiveComparison()
                .isEqualTo(new LogReplicator<>(log, CURRENT_TERM, cluster, MAX_BATCH_SIZE, replicationState));
    }
}