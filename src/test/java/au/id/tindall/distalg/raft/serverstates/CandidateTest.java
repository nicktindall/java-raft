package au.id.tindall.distalg.raft.serverstates;

import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.driver.ElectionScheduler;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.rpc.server.AppendEntriesRequest;
import au.id.tindall.distalg.raft.rpc.server.InitiateElectionMessage;
import au.id.tindall.distalg.raft.rpc.server.RequestVoteResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;
import java.util.Set;

import static au.id.tindall.distalg.raft.serverstates.Result.complete;
import static au.id.tindall.distalg.raft.serverstates.Result.incomplete;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CandidateTest {

    private static final long SERVER_ID = 100L;
    private static final long OTHER_SERVER_ID = 101L;
    private static final Term TERM_0 = new Term(0);
    private static final Term TERM_1 = new Term(1);
    private static final Term TERM_2 = new Term(2);

    @Mock
    private Cluster<Long> cluster;
    @Mock
    private ServerStateFactory<Long> serverStateFactory;
    @Mock
    private Leader<Long> leader;
    @Mock
    private Follower<Long> follower;
    @Mock
    private Log log;
    @Mock
    private ElectionScheduler<Long> electionScheduler;

    @Test
    void receivedVotes_WillBeInitializedEmpty() {
        Candidate<Long> candidateState = new Candidate<>(TERM_2, log, cluster, SERVER_ID, serverStateFactory, electionScheduler);
        assertThat(candidateState.getReceivedVotes()).isEmpty();
    }

    @Test
    void votedFor_WillBeInitializedToOwnId() {
        Candidate<Long> candidateState = new Candidate<>(TERM_2, log, cluster, SERVER_ID, serverStateFactory, electionScheduler);
        assertThat(candidateState.getVotedFor()).contains(SERVER_ID);
    }

    @Test
    void getReceivedVotes_WillReturnUnmodifiableSet() {
        Candidate<Long> candidateState = new Candidate<>(TERM_2, log, cluster, SERVER_ID, serverStateFactory, electionScheduler);
        assertThatCode(
                () -> candidateState.getReceivedVotes().add(OTHER_SERVER_ID)
        ).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void requestVotes_WillBroadcastRequestVoteRpc() {
        Candidate<Long> candidateState = new Candidate<>(TERM_2, log, cluster, SERVER_ID, serverStateFactory, electionScheduler);
        candidateState.requestVotes();
        verify(cluster).sendRequestVoteRequest(TERM_2, 0, Optional.empty());
    }

    @Nested
    class OnEnterState {

        Candidate<Long> candidateState;

        @BeforeEach
        void setUp() {
            candidateState = new Candidate<>(TERM_2, log, cluster, SERVER_ID, serverStateFactory, electionScheduler);
            candidateState.enterState();
        }

        @Test
        void willStartElectionTimeouts() {
            verify(electionScheduler).startTimeouts();
        }
    }

    @Nested
    class OnLeaveState {

        Candidate<Long> candidateState;

        @BeforeEach
        void setUp() {
            candidateState = new Candidate<>(TERM_2, log, cluster, SERVER_ID, serverStateFactory, electionScheduler);
            candidateState.leaveState();
        }

        @Test
        void willStartElectionTimeouts() {
            verify(electionScheduler).stopTimeouts();
        }
    }

    @Nested
    class HandleRequestVoteResponse {

        private Candidate<Long> candidateState;
        private Result<Long> result;

        @BeforeEach
        void setUp() {
            when(cluster.isQuorum(Set.of(SERVER_ID))).thenReturn(false);

            candidateState = new Candidate<>(TERM_2, log, cluster, SERVER_ID, serverStateFactory, electionScheduler);
            candidateState.recordVoteAndClaimLeadershipIfEligible(SERVER_ID);
        }

        @Nested
        class WhenAQuorumIsNotReached {

            @BeforeEach
            void setUp() {
                when(cluster.isQuorum(Set.of(OTHER_SERVER_ID, SERVER_ID))).thenReturn(false);

                result = candidateState.handle(new RequestVoteResponse<>(TERM_2, OTHER_SERVER_ID, SERVER_ID, true));
            }

            @Test
            void willRecordVote() {
                assertThat(candidateState.getReceivedVotes()).contains(OTHER_SERVER_ID);
            }

            @Test
            void willRemainInCandidateState() {
                assertThat(result).isEqualToComparingFieldByField(complete(candidateState));
            }
        }

        @Nested
        class WhenAQuorumIsReached {

            @BeforeEach
            void setUp() {
                when(cluster.isQuorum(Set.of(OTHER_SERVER_ID, SERVER_ID))).thenReturn(true);
                when(serverStateFactory.createLeader(TERM_2)).thenReturn(leader);

                result = candidateState.handle(new RequestVoteResponse<>(TERM_2, OTHER_SERVER_ID, SERVER_ID, true));
            }

            @Test
            void willTransitionToLeaderState() {
                assertThat(result).usingRecursiveComparison().isEqualTo(complete(leader));
            }
        }

        @Test
        void willIgnoreResponse_WhenItIsStale() {
            reset(cluster);
            Result<Long> result = candidateState.handle(new RequestVoteResponse<>(TERM_1, OTHER_SERVER_ID, SERVER_ID, true));

            verifyNoMoreInteractions(serverStateFactory);
            verify(cluster, never()).isQuorum(anySet());
            assertThat(candidateState.getReceivedVotes()).doesNotContain(OTHER_SERVER_ID);
            assertThat(result).usingRecursiveComparison().isEqualTo(complete(candidateState));
        }
    }

    @Nested
    class HandleAppendEntriesRequest {

        @Test
        void willTransitionToFollowerAndContinueProcessingMessage_WhenTermIsGreaterThanOrEqualToLocalTerm() {
            when(serverStateFactory.createFollower(TERM_2, OTHER_SERVER_ID)).thenReturn(follower);

            Candidate<Long> candidateState = new Candidate<>(TERM_1, log, cluster, SERVER_ID, serverStateFactory, electionScheduler);
            Result<Long> result = candidateState.handle(new AppendEntriesRequest<>(TERM_2, OTHER_SERVER_ID, SERVER_ID, 2, Optional.of(TERM_0), emptyList(), 0));

            assertThat(result).usingRecursiveComparison().isEqualTo(incomplete(follower));
        }

        @Test
        void willRespondUnsuccessful_WhenRequestIsStale() {
            Candidate<Long> candidateState = new Candidate<>(TERM_1, log, cluster, SERVER_ID, serverStateFactory, electionScheduler);
            Result<Long> result = candidateState.handle(new AppendEntriesRequest<>(TERM_0, OTHER_SERVER_ID, SERVER_ID, 2, Optional.of(TERM_0), emptyList(), 0));

            assertThat(result).usingRecursiveComparison().isEqualTo(complete(candidateState));
            verify(cluster).sendAppendEntriesResponse(TERM_1, OTHER_SERVER_ID, false, Optional.empty());
        }
    }

    @Nested
    class HandleInitializeElectionMessage {

        @Nested
        class WhenOwnVoteIsNotQuorum {

            private static final int LAST_LOG_INDEX = 2631;
            private Candidate<Long> candidate;

            @BeforeEach
            void setUp() {
                when(cluster.isQuorum(Set.of(SERVER_ID))).thenReturn(false);
                when(log.getLastLogIndex()).thenReturn(LAST_LOG_INDEX);
                when(log.getLastLogTerm()).thenReturn(Optional.of(TERM_0));
                candidate = new Candidate<>(TERM_1, log, cluster, SERVER_ID, serverStateFactory, electionScheduler);
            }

            @Test
            void willRemainIndCandidateStateAndCompleteProcessing() {
                assertThat(candidate.handle(new InitiateElectionMessage<>(TERM_1, SERVER_ID))).usingRecursiveComparison().isEqualTo(complete(candidate));
            }

            @Test
            void willRequestVotes() {
                candidate.handle(new InitiateElectionMessage<>(TERM_1, SERVER_ID));
                verify(cluster).sendRequestVoteRequest(TERM_1, LAST_LOG_INDEX, Optional.of(TERM_0));
            }

            @Test
            void willResetElectionTimeout() {
                candidate.handle(new InitiateElectionMessage<>(TERM_1, SERVER_ID));
                verify(electionScheduler).resetTimeout();
            }
        }

        @Nested
        class WhenOwnVoteIsQuorum {

            private Candidate<Long> candidate;

            @BeforeEach
            void setUp() {
                when(cluster.isQuorum(Set.of(SERVER_ID))).thenReturn(true);
                when(serverStateFactory.createLeader(TERM_1)).thenReturn(leader);
                candidate = new Candidate<>(TERM_1, log, cluster, SERVER_ID, serverStateFactory, electionScheduler);
            }

            @Test
            void willTransitionToLeaderAndCompleteProcessing() {
                assertThat(candidate.handle(new InitiateElectionMessage<>(TERM_1, SERVER_ID))).usingRecursiveComparison().isEqualTo(complete(leader));
            }

            @Test
            void willNotRequestVotes() {
                candidate.handle(new InitiateElectionMessage<>(TERM_1, SERVER_ID));
                verify(cluster, never()).sendRequestVoteRequest(any(), anyInt(), any());
            }

            @Test
            void willResetElectionTimeout() {
                candidate.handle(new InitiateElectionMessage<>(TERM_1, SERVER_ID));
                verify(electionScheduler).resetTimeout();
            }
        }
    }
}