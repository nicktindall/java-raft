package au.id.tindall.distalg.raft;

import static au.id.tindall.distalg.raft.ServerState.FOLLOWER;
import static org.assertj.core.api.Assertions.assertThat;

import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.Term;
import org.junit.Test;

public class ServerTest {

    private static final long SERVER_ID = 100L;
    private static final Term RESTORED_TERM = new Term(111);
    private static final long RESTORED_VOTED_FOR = 999L;
    private static final Log RESTORED_LOG = new Log();

    @Test
    public void newServerConstructor_WillSetId() {
        Server<Long> server = new Server<>(SERVER_ID);
        assertThat(server.getId()).isEqualTo(SERVER_ID);
    }

    @Test
    public void newServerConstructor_WillInitializeCurrentTermToZero() {
        Server<Long> server = new Server<>(SERVER_ID);
        assertThat(server.getCurrentTerm()).isEqualTo(new Term(0));
    }

    @Test
    public void newServerConstructor_WillInitializeLogToEmpty() {
        Server<Long> server = new Server<>(SERVER_ID);
        assertThat(server.getLog().getEntries()).isEmpty();
    }

    @Test
    public void newServerConstructor_WillInitializeVotedForToNull() {
        Server<Long> server = new Server<>(SERVER_ID);
        assertThat(server.getVotedFor()).isNull();
    }

    @Test
    public void newServerConstructor_WillInitializeStateToFollower() {
        Server<Long> server = new Server<>(SERVER_ID);
        assertThat(server.getState()).isEqualTo(FOLLOWER);
    }

    @Test
    public void resetServerConstructor_WillSetId() {
        Server<Long> server = new Server<>(SERVER_ID, RESTORED_TERM, RESTORED_VOTED_FOR, RESTORED_LOG);
        assertThat(server.getId()).isEqualTo(SERVER_ID);
    }

    @Test
    public void resetServerConstructor_WillRestoreCurrentTerm() {
        Server<Long> server = new Server<>(SERVER_ID, RESTORED_TERM, RESTORED_VOTED_FOR, RESTORED_LOG);
        assertThat(server.getCurrentTerm()).isEqualTo(RESTORED_TERM);
    }

    @Test
    public void resetServerConstructor_WillRestoreLog() {
        Server<Long> server = new Server<>(SERVER_ID, RESTORED_TERM, RESTORED_VOTED_FOR, RESTORED_LOG);
        assertThat(server.getLog()).isSameAs(RESTORED_LOG);
    }

    @Test
    public void resetServerConstructor_WillRestoreVotedFor() {
        Server<Long> server = new Server<>(SERVER_ID, RESTORED_TERM, RESTORED_VOTED_FOR, RESTORED_LOG);
        assertThat(server.getVotedFor()).isEqualTo(RESTORED_VOTED_FOR);
    }

    @Test
    public void resetServerConstructor_WillInitializeStateToFollower() {
        Server<Long> server = new Server<>(SERVER_ID, RESTORED_TERM, RESTORED_VOTED_FOR, RESTORED_LOG);
        assertThat(server.getState()).isEqualTo(FOLLOWER);
    }
}