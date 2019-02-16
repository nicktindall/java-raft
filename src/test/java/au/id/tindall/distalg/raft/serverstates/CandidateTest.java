package au.id.tindall.distalg.raft.serverstates;

import static au.id.tindall.distalg.raft.DomainUtils.logContaining;
import static au.id.tindall.distalg.raft.serverstates.Result.complete;
import static au.id.tindall.distalg.raft.serverstates.Result.incomplete;
import static au.id.tindall.distalg.raft.serverstates.ServerStateType.FOLLOWER;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.refEq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.Set;

import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.LogEntry;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.rpc.AppendEntriesRequest;
import au.id.tindall.distalg.raft.rpc.RequestVoteRequest;
import au.id.tindall.distalg.raft.rpc.RequestVoteResponse;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CandidateTest {

    private static final long SERVER_ID = 100L;
    private static final long OTHER_SERVER_ID = 101L;
    private static final Term TERM_0 = new Term(0);
    private static final Term TERM_1 = new Term(1);
    private static final Term TERM_2 = new Term(2);
    private static final LogEntry ENTRY_1 = new LogEntry(TERM_0, "first".getBytes());
    private static final LogEntry ENTRY_2 = new LogEntry(TERM_0, "second".getBytes());
    private static final LogEntry ENTRY_3 = new LogEntry(TERM_1, "third".getBytes());

    @Mock
    private Cluster<Long> cluster;

    @Test
    public void receivedVotes_WillBeInitializedEmpty() {
        Candidate<Long> candidateState = new Candidate<>(SERVER_ID, TERM_2, new Log(), cluster);
        assertThat(candidateState.getReceivedVotes()).isEmpty();
    }

    @Test
    public void votedFor_WillBeInitializedToOwnId() {
        Candidate<Long> candidateState = new Candidate<>(SERVER_ID, TERM_2, new Log(), cluster);
        assertThat(candidateState.getVotedFor()).contains(SERVER_ID);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void getReceivedVotes_WillReturnUnmodifiableSet() {
        Candidate<Long> candidateState = new Candidate<>(SERVER_ID, TERM_2, new Log(), cluster);
        candidateState.getReceivedVotes().add(OTHER_SERVER_ID);
    }

    @Test
    public void requestVotes_WillBroadcastRequestVoteRpc() {
        Candidate<Long> candidateState = new Candidate<>(SERVER_ID, TERM_2, new Log(), cluster);
        candidateState.requestVotes();
        verify(cluster).send(refEq(new RequestVoteRequest<>(TERM_2, SERVER_ID, 0, Optional.empty())));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void handleRequestVoteResponse_WillRecordVote_WhenResponseIsNotStaleAndQuorumIsNotReached() {
        Candidate<Long> candidateState = new Candidate<>(SERVER_ID, TERM_2, logContaining(ENTRY_1, ENTRY_2, ENTRY_3), cluster);
        when(cluster.isQuorum(anySet())).thenReturn(false);
        Result<Long> result = candidateState.handle(new RequestVoteResponse<>(TERM_2, OTHER_SERVER_ID, SERVER_ID, true));
        verify(cluster, never()).send(any(AppendEntriesRequest.class));
        assertThat(result).isEqualToComparingFieldByField(complete(candidateState));
        assertThat(candidateState.getReceivedVotes()).contains(OTHER_SERVER_ID);
    }

    @Test
    public void handleRequestVoteResponse_WillTransitionToLeaderStateAndSendHeartbeat_WhenAQuorumIsReached() {
        Log log = logContaining(ENTRY_1, ENTRY_2, ENTRY_3);
        Candidate<Long> candidateState = new Candidate<>(SERVER_ID, TERM_2, log, cluster);
        when(cluster.isQuorum(Set.of(SERVER_ID))).thenReturn(true);
        when(cluster.getMemberIds()).thenReturn(Set.of(SERVER_ID));
        Result<Long> result = candidateState.handle(new RequestVoteResponse<>(TERM_2, SERVER_ID, SERVER_ID, true));
        verify(cluster).send(refEq(new AppendEntriesRequest<>(TERM_2, SERVER_ID, 3, Optional.of(TERM_1), emptyList(), 0)));
        assertThat(result).isEqualToComparingFieldByFieldRecursively(complete(new Leader<>(SERVER_ID, TERM_2, log, cluster)));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void handleRequestVoteResponse_WillIgnoreResponse_WhenItIsStale() {
        Candidate<Long> candidateState = new Candidate<>(SERVER_ID, TERM_2, logContaining(ENTRY_1, ENTRY_2, ENTRY_3), cluster);
        Result<Long> result = candidateState.handle(new RequestVoteResponse<>(TERM_1, OTHER_SERVER_ID, SERVER_ID, true));
        verify(cluster, never()).send(any(AppendEntriesRequest.class));
        verify(cluster, never()).isQuorum(anySet());
        assertThat(candidateState.getReceivedVotes()).doesNotContain(OTHER_SERVER_ID);
        assertThat(result).isEqualToComparingFieldByFieldRecursively(complete(candidateState));
    }

    @Test
    public void handleAppendEntriesRequest_WillTransitionToFollowerAndContinueProcessingMessage_WhenTermIsGreaterThanOrEqualToLocalTerm() {
        Log log = logContaining(ENTRY_1, ENTRY_2);
        Candidate<Long> server = new Candidate<>(SERVER_ID, TERM_1, log, cluster);
        Result<Long> result = server.handle(new AppendEntriesRequest<>(TERM_2, OTHER_SERVER_ID, 2, Optional.of(TERM_0), emptyList(), 0));
        assertThat(result).isEqualToComparingFieldByFieldRecursively(incomplete(new ServerState<>(SERVER_ID, TERM_2, FOLLOWER, null, log, cluster)));
    }
}