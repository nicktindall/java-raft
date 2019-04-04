package au.id.tindall.distalg.raft.serverstates;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import au.id.tindall.distalg.raft.client.PendingResponseRegistryFactory;
import au.id.tindall.distalg.raft.client.PendingResponseRegistry;
import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.LogEntry;
import au.id.tindall.distalg.raft.replication.LogReplicator;
import au.id.tindall.distalg.raft.replication.LogReplicatorFactory;
import au.id.tindall.distalg.raft.rpc.client.RegisterClientRequest;
import au.id.tindall.distalg.raft.rpc.server.AppendEntriesResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class LeaderTest {

    private static final long SERVER_ID = 111;
    private static final long OTHER_SERVER_ID = 112;
    private static final Term TERM_1 = new Term(1);
    private static final Term TERM_2 = new Term(2);
    private static final int NEXT_LOG_INDEX = 4;
    private static final int LAST_LOG_INDEX = 3;

    @Mock
    private Cluster<Long> cluster;
    @Mock
    private PendingResponseRegistryFactory pendingResponseRegistryFactory;
    @Mock
    private LogReplicatorFactory<Long> logReplicatorFactory;
    @Mock
    private PendingResponseRegistry pendingResponseRegistry;
    @Mock
    private Log log;
    @Mock
    private LogReplicator<Long> otherServerLogReplicator;

    private Leader<Long> leader;

    @BeforeEach
    public void setUp() {
        when(log.getNextLogIndex()).thenReturn(NEXT_LOG_INDEX);
        when(pendingResponseRegistryFactory.createPendingResponseRegistry()).thenReturn(pendingResponseRegistry);
        when(cluster.getMemberIds()).thenReturn(Set.of(SERVER_ID, OTHER_SERVER_ID));
        when(logReplicatorFactory.createLogReplicator(SERVER_ID, cluster, OTHER_SERVER_ID, NEXT_LOG_INDEX)).thenReturn(otherServerLogReplicator);
        leader = new Leader<>(SERVER_ID, TERM_2, log, cluster, pendingResponseRegistryFactory, logReplicatorFactory);
    }

    @Nested
    class Constructor {

        @Test
        public void willCreateLogReplicators() {
            verify(logReplicatorFactory).createLogReplicator(SERVER_ID, cluster, OTHER_SERVER_ID, NEXT_LOG_INDEX);
        }

        @Test
        public void willAddClientRegistryFactoryAsEntryCommittedEventHandler() {
            verify(pendingResponseRegistry).startListeningForCommitEvents(log);
        }
    }

    @Test
    public void dispose_WillStopListeningForCommittedEntriesThenFailAnyOutstandingRegistrations() {
        reset(pendingResponseRegistry);
        leader.dispose();
        InOrder inOrder = inOrder(pendingResponseRegistry);
        inOrder.verify(pendingResponseRegistry).stopListeningForCommitEvents(log);
        inOrder.verify(pendingResponseRegistry).failOutstandingResponses();
        inOrder.verifyNoMoreInteractions();
    }

    @Nested
    class HandleAppendEntriesResponse {

        @Test
        public void willIgnoreMessage_WhenItIsStale() {
            leader.handle(new AppendEntriesResponse<>(TERM_1, OTHER_SERVER_ID, SERVER_ID, true, Optional.of(5)));
            verifyZeroInteractions(otherServerLogReplicator, log);
        }

        @Test
        public void willLogSuccessResponseWithReplicatorThenUpdateCommitIndex_WhenResultIsSuccess() {
            int otherServerMatchIndex = 3;
            when(otherServerLogReplicator.getMatchIndex()).thenReturn(otherServerMatchIndex);

            leader.handle(new AppendEntriesResponse<>(TERM_2, OTHER_SERVER_ID, SERVER_ID, true, Optional.of(5)));
            InOrder inOrder = inOrder(otherServerLogReplicator, log);
            inOrder.verify(otherServerLogReplicator).logSuccessResponse(5);
            inOrder.verify(log).updateCommitIndex(List.of(otherServerMatchIndex));
        }

        @Test
        public void willLogFailureResponseWithReplicator_WhenResultIsFail() {
            leader.handle(new AppendEntriesResponse<>(TERM_2, OTHER_SERVER_ID, SERVER_ID, false, Optional.empty()));
            verify(otherServerLogReplicator).logFailedResponse();
        }

        @Test
        public void willNotUpdateCommitIndex_WhenResultIsFail() {
            leader.handle(new AppendEntriesResponse<>(TERM_2, OTHER_SERVER_ID, SERVER_ID, false, Optional.empty()));
            verify(log, never()).updateCommitIndex(anyList());
        }
    }

    @Nested
    class HandleRegisterClientRequest {

        @BeforeEach
        void setUp() {
            when(log.getLastLogIndex()).thenReturn(LAST_LOG_INDEX);
        }

        @Test
        @SuppressWarnings("unchecked")
        void willAppendClientRegistrationLogEntry() {
            leader.handle(new RegisterClientRequest<>(SERVER_ID));

            ArgumentCaptor<List<LogEntry>> captor = ArgumentCaptor.forClass(List.class);
            verify(log).appendEntries(eq(3), captor.capture());
            assertThat(captor.getValue()).usingFieldByFieldElementComparator().isEqualTo(captor.getValue());
        }

        @Test
        void willTriggerReplication() {
            leader.handle(new RegisterClientRequest<>(SERVER_ID));


        }
    }
}