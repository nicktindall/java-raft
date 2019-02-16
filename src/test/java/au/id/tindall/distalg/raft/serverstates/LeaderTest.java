package au.id.tindall.distalg.raft.serverstates;

import static au.id.tindall.distalg.raft.DomainUtils.logContaining;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.refEq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.log.LogEntry;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.rpc.AppendEntriesRequest;
import au.id.tindall.distalg.raft.rpc.AppendEntriesResponse;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class LeaderTest {

    private static final long SERVER_ID = 111;
    private static final long OTHER_SERVER_ID = 112;
    private static final Term TERM_0 = new Term(0);
    private static final Term TERM_1 = new Term(1);
    private static final Term TERM_2 = new Term(2);
    private static final LogEntry ENTRY_1 = new LogEntry(TERM_0, "first".getBytes());
    private static final LogEntry ENTRY_2 = new LogEntry(TERM_0, "second".getBytes());
    private static final LogEntry ENTRY_3 = new LogEntry(TERM_1, "third".getBytes());

    @Mock
    private Cluster<Long> cluster;

    @Before
    public void setUp() {
        when(cluster.getMemberIds()).thenReturn(Set.of(SERVER_ID, OTHER_SERVER_ID));
    }

    @Test
    public void constructor_WillInitializeLeaderState() {
        Leader<Long> leader = electedLeader();
        assertThat(leader.getNextIndices()).isEqualTo(Map.of(SERVER_ID, 4, OTHER_SERVER_ID, 4));
        assertThat(leader.getMatchIndices()).isEqualTo(Map.of(SERVER_ID, 0, OTHER_SERVER_ID, 0));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void getNextIndices_WillReturnUnmodifiableMap() {
        Leader<Long> leader = electedLeader();
        leader.getNextIndices().put(123L, 456);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void getMatchIndices_WillReturnUnmodifiableMap() {
        Leader<Long> leader = electedLeader();
        leader.getMatchIndices().put(123L, 456);
    }

    @Test
    public void handleAppendEntriesResponse_WillIgnoreMessage_WhenItIsStale() {
        Leader<Long> leader = electedLeader();
        leader.handle(new AppendEntriesResponse<>(TERM_1, OTHER_SERVER_ID, SERVER_ID, true, Optional.of(5)));
        assertThat(leader.getNextIndices().get(OTHER_SERVER_ID)).isEqualTo(4);
        assertThat(leader.getMatchIndices().get(OTHER_SERVER_ID)).isEqualTo(0);
    }

    @Test
    public void handleAppendEntriesResponse_WillUpdateNextIndex_WhenResultIsSuccess() {
        Leader<Long> leader = electedLeader();
        leader.handle(new AppendEntriesResponse<>(TERM_2, OTHER_SERVER_ID, SERVER_ID, true, Optional.of(5)));
        assertThat(leader.getNextIndices().get(OTHER_SERVER_ID)).isEqualTo(6);
    }

    @Test
    public void handleAppendEntriesResponse_WillUpdateMatchIndex_WhenResultIsSuccess() {
        Leader<Long> leader = electedLeader();
        leader.handle(new AppendEntriesResponse<>(TERM_2, OTHER_SERVER_ID, SERVER_ID, true, Optional.of(5)));
        assertThat(leader.getMatchIndices().get(OTHER_SERVER_ID)).isEqualTo(5);
    }

    @Test
    public void handleAppendEntriesResponse_WillDecrementNextIndex_WhenResultIsFail() {
        Leader<Long> leader = electedLeader();
        leader.handle(new AppendEntriesResponse<>(TERM_2, OTHER_SERVER_ID, SERVER_ID, false, Optional.empty()));
        assertThat(leader.getNextIndices().get(OTHER_SERVER_ID)).isEqualTo(3);
    }

    @Test
    public void handleAppendEntriesResponse_WillNotUpdateMatchIndex_WhenResultIsFail() {
        Leader<Long> leader = electedLeader();
        leader.handle(new AppendEntriesResponse<>(TERM_2, OTHER_SERVER_ID, SERVER_ID, false, Optional.empty()));
        assertThat(leader.getMatchIndices().get(OTHER_SERVER_ID)).isEqualTo(0);
    }

    @Test
    public void sendHeartbeatRequest_WillDispatchEmptyAppendLogEntriesMessage() {
        Leader<Long> leader = electedLeader();
        leader.sendHeartbeatMessage();
        verify(cluster).send(refEq(new AppendEntriesRequest<>(TERM_2, SERVER_ID, 3, Optional.of(TERM_1), emptyList(), 0)));
    }

    private Leader<Long> electedLeader() {
        return new Leader<>(SERVER_ID, TERM_2, logContaining(ENTRY_1, ENTRY_2, ENTRY_3), cluster);
    }
}