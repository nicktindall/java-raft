package au.id.tindall.distalg.raft;

import static au.id.tindall.distalg.raft.ServerState.CANDIDATE;
import static au.id.tindall.distalg.raft.ServerState.FOLLOWER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.refEq;
import static org.mockito.Mockito.verify;

import au.id.tindall.distalg.raft.comms.MessageDispatcher;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.rpc.RequestVoteRequest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ServerTest {

    private static final long SERVER_ID = 100L;
    private static final Term RESTORED_TERM = new Term(111);
    private static final long RESTORED_VOTED_FOR = 999L;
    private static final Log RESTORED_LOG = new Log();

    @Mock
    private MessageDispatcher<Long> messageDispatcher;

    @Test
    public void newServerConstructor_WillSetId() {
        Server<Long> server = new Server<>(SERVER_ID, messageDispatcher);
        assertThat(server.getId()).isEqualTo(SERVER_ID);
    }

    @Test
    public void newServerConstructor_WillInitializeCurrentTermToZero() {
        Server<Long> server = new Server<>(SERVER_ID, messageDispatcher);
        assertThat(server.getCurrentTerm()).isEqualTo(new Term(0));
    }

    @Test
    public void newServerConstructor_WillInitializeLogToEmpty() {
        Server<Long> server = new Server<>(SERVER_ID, messageDispatcher);
        assertThat(server.getLog().getEntries()).isEmpty();
    }

    @Test
    public void newServerConstructor_WillInitializeVotedForToNull() {
        Server<Long> server = new Server<>(SERVER_ID, messageDispatcher);
        assertThat(server.getVotedFor()).isNull();
    }

    @Test
    public void newServerConstructor_WillInitializeStateToFollower() {
        Server<Long> server = new Server<>(SERVER_ID, messageDispatcher);
        assertThat(server.getState()).isEqualTo(FOLLOWER);
    }

    @Test
    public void resetServerConstructor_WillSetId() {
        Server<Long> server = new Server<>(SERVER_ID, RESTORED_TERM, RESTORED_VOTED_FOR, RESTORED_LOG, messageDispatcher);
        assertThat(server.getId()).isEqualTo(SERVER_ID);
    }

    @Test
    public void resetServerConstructor_WillRestoreCurrentTerm() {
        Server<Long> server = new Server<>(SERVER_ID, RESTORED_TERM, RESTORED_VOTED_FOR, RESTORED_LOG, messageDispatcher);
        assertThat(server.getCurrentTerm()).isEqualTo(RESTORED_TERM);
    }

    @Test
    public void resetServerConstructor_WillRestoreLog() {
        Server<Long> server = new Server<>(SERVER_ID, RESTORED_TERM, RESTORED_VOTED_FOR, RESTORED_LOG, messageDispatcher);
        assertThat(server.getLog()).isSameAs(RESTORED_LOG);
    }

    @Test
    public void resetServerConstructor_WillRestoreVotedFor() {
        Server<Long> server = new Server<>(SERVER_ID, RESTORED_TERM, RESTORED_VOTED_FOR, RESTORED_LOG, messageDispatcher);
        assertThat(server.getVotedFor()).isEqualTo(RESTORED_VOTED_FOR);
    }

    @Test
    public void resetServerConstructor_WillInitializeStateToFollower() {
        Server<Long> server = new Server<>(SERVER_ID, RESTORED_TERM, RESTORED_VOTED_FOR, RESTORED_LOG, messageDispatcher);
        assertThat(server.getState()).isEqualTo(FOLLOWER);
    }

    @Test
    public void electionTimeout_WillSetStateToCandidate() {
        Server<Long> server = new Server<>(SERVER_ID, messageDispatcher);
        server.electionTimeout();
        assertThat(server.getState()).isEqualTo(CANDIDATE);
    }

    @Test
    public void electionTimeout_WillIncrementTerm() {
        Server<Long> server = new Server<>(SERVER_ID, RESTORED_TERM, RESTORED_VOTED_FOR, RESTORED_LOG, messageDispatcher);
        server.electionTimeout();
        assertThat(server.getCurrentTerm()).isEqualTo(RESTORED_TERM.next());
    }

    @Test
    public void electionTimeout_WillVoteForSelf() {
        Server<Long> server = new Server<>(SERVER_ID, RESTORED_TERM, RESTORED_VOTED_FOR, RESTORED_LOG, messageDispatcher);
        server.electionTimeout();
        assertThat(server.getVotedFor()).isEqualTo(SERVER_ID);
    }

    @Test
    public void electionTimeout_WillBroadcastRequestVoteRequests() {
        Server<Long> server = new Server<>(SERVER_ID, RESTORED_TERM, RESTORED_VOTED_FOR, RESTORED_LOG, messageDispatcher);
        server.electionTimeout();
        verify(messageDispatcher).broadcastMessage(refEq(new RequestVoteRequest<>(RESTORED_TERM.next(), SERVER_ID, RESTORED_LOG.getLastLogIndex(), RESTORED_LOG.getLastLogTerm())));
    }
}