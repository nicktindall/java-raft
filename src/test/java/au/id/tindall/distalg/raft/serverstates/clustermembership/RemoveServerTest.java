package au.id.tindall.distalg.raft.serverstates.clustermembership;

import au.id.tindall.distalg.raft.cluster.Configuration;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.ConfigurationEntry;
import au.id.tindall.distalg.raft.log.entries.LogEntry;
import au.id.tindall.distalg.raft.replication.ReplicationManager;
import au.id.tindall.distalg.raft.rpc.clustermembership.RemoveServerResponse;
import au.id.tindall.distalg.raft.state.PersistentState;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

@ExtendWith(MockitoExtension.class)
public class RemoveServerTest {

    private static final Term CURRENT_TERM = new Term(1);
    private static final int SERVER_ID = 111;
    private static final int OTHER_SERVER_ID1 = 222;
    private static final int OTHER_SERVER_ID2 = 333;
    private static final int LAST_LOG_INDEX = 1234;
    private static final int APPENDED_LOG_INDEX = LAST_LOG_INDEX + 1;

    @Mock
    private Log log;
    @Mock
    private Configuration<Integer> configuration;
    @Mock
    private PersistentState<Integer> persistentState;
    @Mock
    private ReplicationManager<Integer> replicationManager;
    @Mock
    private Supplier<Instant> timeSource;

    private RemoveServer<Integer> removeServer;

    @BeforeEach
    void setUp() {
        lenient().when(configuration.getServers()).thenReturn(Set.of(SERVER_ID, OTHER_SERVER_ID1, OTHER_SERVER_ID2));
        lenient().when(log.getLastLogIndex()).thenReturn(LAST_LOG_INDEX, APPENDED_LOG_INDEX);
        lenient().when(persistentState.getCurrentTerm()).thenReturn(CURRENT_TERM);
        removeServer = new RemoveServer<>(log, configuration, persistentState, replicationManager, SERVER_ID, timeSource);
    }

    @SuppressWarnings("unchecked")
    @Test
    void startWillAppendConfigurationUpdateWithServerRemoved() {
        removeServer.start();
        final ArgumentCaptor<List<LogEntry>> listArgumentCaptor = ArgumentCaptor.forClass(List.class);
        verify(log).appendEntries(eq(LAST_LOG_INDEX), listArgumentCaptor.capture());
        assertThat(listArgumentCaptor.getValue()).usingRecursiveComparison()
                .isEqualTo(List.of(new ConfigurationEntry(CURRENT_TERM, Set.of(OTHER_SERVER_ID1, OTHER_SERVER_ID2))));
    }

    @Nested
    class EntryCommitted {

        @BeforeEach
        void setUp() {
            removeServer.start();
        }

        @Test
        void willStopReplicatingToServer() {
            verifyNoInteractions(replicationManager);
            removeServer.entryCommitted(LAST_LOG_INDEX);
            verifyNoInteractions(replicationManager);
            removeServer.entryCommitted(APPENDED_LOG_INDEX);
            verify(replicationManager).stopReplicatingTo(SERVER_ID);
        }

        @Test
        void willResolveResponseFutureWithOK() {
            assertThat(removeServer.getResponseFuture()).isNotCompleted();
            removeServer.entryCommitted(LAST_LOG_INDEX);
            assertThat(removeServer.getResponseFuture()).isNotCompleted();
            removeServer.entryCommitted(APPENDED_LOG_INDEX);
            assertThat(removeServer.getResponseFuture()).isCompletedWithValue(RemoveServerResponse.OK);
        }
    }

    @Nested
    class Close {

        @Test
        void willCompleteFutureWithNotLeader() {
            removeServer.close();
            Assertions.assertThat(removeServer.getResponseFuture()).isCompletedWithValue(RemoveServerResponse.NOT_LEADER);
        }
    }

}
