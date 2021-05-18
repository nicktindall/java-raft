package au.id.tindall.distalg.raft.serverstates.clustermembership;

import au.id.tindall.distalg.raft.cluster.Configuration;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.ConfigurationEntry;
import au.id.tindall.distalg.raft.log.entries.LogEntry;
import au.id.tindall.distalg.raft.rpc.clustermembership.RemoveServerResponse;
import au.id.tindall.distalg.raft.state.PersistentState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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

    private RemoveServer<Integer> removeServer;

    @BeforeEach
    void setUp() {
        when(configuration.getServers()).thenReturn(Set.of(SERVER_ID, OTHER_SERVER_ID1, OTHER_SERVER_ID2));
        when(log.getLastLogIndex()).thenReturn(LAST_LOG_INDEX, APPENDED_LOG_INDEX);
        when(persistentState.getCurrentTerm()).thenReturn(CURRENT_TERM);
        removeServer = new RemoveServer<>(log, configuration, persistentState, SERVER_ID);
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

    @Test
    void willResolveResponseFutureWithOKOnlyWhenEntryIsCommitted() {
        removeServer.start();
        assertThat(removeServer.getResponseFuture()).isNotCompleted();
        removeServer.entryCommitted(LAST_LOG_INDEX);
        assertThat(removeServer.getResponseFuture()).isNotCompleted();
        removeServer.entryCommitted(APPENDED_LOG_INDEX);
        assertThat(removeServer.getResponseFuture()).isCompletedWithValue(RemoveServerResponse.OK);
    }
}
