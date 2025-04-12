package au.id.tindall.distalg.raft.serverstates.clustermembership;

import au.id.tindall.distalg.raft.cluster.Configuration;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.ConfigurationEntry;
import au.id.tindall.distalg.raft.log.entries.LogEntry;
import au.id.tindall.distalg.raft.replication.ReplicationManager;
import au.id.tindall.distalg.raft.rpc.clustermembership.AddServerResponse;
import au.id.tindall.distalg.raft.state.PersistentState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class AddServerTest {

    private static final Term CURRENT_TERM = new Term(1);
    private static final int SERVER_ID = 111;
    private static final int OTHER_SERVER_ID1 = 222;
    private static final int OTHER_SERVER_ID2 = 333;
    private static final int FIRST_ROUND_END = 1234;
    private static final int SECOND_ROUND_END = 5678;
    private static final int APPEND_INDEX = 6789;
    private static final Duration ELECTION_TIMEOUT = Duration.ofMillis(1000);
    private static final int NUMBER_OF_CATCHUP_ROUNDS = 2;
    private static final Instant NOW = Instant.now();

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

    private AddServer<Integer> addServer;

    @BeforeEach
    void setUp() {
        when(timeSource.get()).thenReturn(NOW);
        lenient().when(configuration.getElectionTimeout()).thenReturn(ELECTION_TIMEOUT);
        lenient().when(persistentState.getCurrentTerm()).thenReturn(CURRENT_TERM);
        when(log.getLastLogIndex()).thenReturn(FIRST_ROUND_END, SECOND_ROUND_END, APPEND_INDEX);
        addServer = new AddServer<>(log, configuration, persistentState,
                replicationManager, SERVER_ID, NUMBER_OF_CATCHUP_ROUNDS, timeSource);
    }

    @Nested
    class Start {

        @Test
        void willRegisterAReplicatorForTheNewServer() {
            addServer.start();
            verify(replicationManager).startReplicatingTo(SERVER_ID);
        }
    }

    @Nested
    class MatchIndexAdvanced {

        @BeforeEach
        void setUp() {
            addServer.start();
        }

        @Test
        void willAdvanceToTheNextRoundWhenCurrentOneIsComplete() {
            addServer.matchIndexAdvancedInternal(FIRST_ROUND_END);
            assertThat(addServer.isFinished()).isFalse();
            verify(log, times(0)).appendEntries(anyInt(), any());
        }

        @Nested
        class OnTheLastRound {

            @BeforeEach
            void setUp() {
                addServer.matchIndexAdvanced(SERVER_ID, FIRST_ROUND_END);
            }

            @Nested
            class WhenTimeoutIsExceeded {

                @Test
                void willFailAdd() {
                    when(timeSource.get()).thenReturn(NOW.plus(Duration.ofSeconds(5)));
                    addServer.matchIndexAdvanced(SERVER_ID, SECOND_ROUND_END);
                    assertThat(addServer.getResponseFuture()).isCompletedWithValue(AddServerResponse.getTimeout());
                }
            }

            @Nested
            class WhenTimeoutIsNotExceeded {

                @SuppressWarnings("unchecked")
                @Test
                void willAppendNewConfigToLog() {
                    when(configuration.getServers()).thenReturn(Set.of(OTHER_SERVER_ID1, OTHER_SERVER_ID2));

                    addServer.matchIndexAdvanced(SERVER_ID, SECOND_ROUND_END);
                    final ArgumentCaptor<List<LogEntry>> logEntryCaptor = ArgumentCaptor.forClass(List.class);
                    verify(log).appendEntries(eq(APPEND_INDEX), logEntryCaptor.capture());
                    assertThat(addServer.finishedAtIndex).isEqualTo(APPEND_INDEX);
                    assertThat(logEntryCaptor.getValue()).usingFieldByFieldElementComparator().containsOnly(
                            new ConfigurationEntry(CURRENT_TERM, Set.of(SERVER_ID, OTHER_SERVER_ID1, OTHER_SERVER_ID2)));
                    assertThat(addServer.getResponseFuture()).isNotCompleted();
                }
            }
        }
    }

    @Nested
    class LogMessageFromFollower {

        @BeforeEach
        void setUp() {
            addServer.start();
        }

        @Nested
        class WhenServerIdMatches {

            @Test
            void willResetInactivityTimeout() {
                when(timeSource.get()).thenReturn(NOW.plus(ELECTION_TIMEOUT.multipliedBy(2)));
                addServer.logMessageFromFollower(SERVER_ID);
                when(timeSource.get()).thenReturn(NOW.plus(ELECTION_TIMEOUT.multipliedBy(4)));
                assertThat(addServer.isFinished()).isFalse();
            }
        }

        @Nested
        class WhenServerIdDoesNotMatch {

            @Test
            void willNotResetInactivityTimeout() {
                when(timeSource.get()).thenReturn(NOW.plus(ELECTION_TIMEOUT.multipliedBy(2)));
                addServer.logMessageFromFollower(OTHER_SERVER_ID1);
                when(timeSource.get()).thenReturn(NOW.plus(ELECTION_TIMEOUT.multipliedBy(4)));
                assertThat(addServer.isFinished()).isTrue();
            }
        }
    }

    @Nested
    class IsFinished {

        @Test
        void willTimeoutAddIfNoSuccessHasBeenReceivedInAges() {
            addServer.start();
            when(timeSource.get()).thenReturn(NOW.plus(ELECTION_TIMEOUT.multipliedBy(2)));
            assertThat(addServer.isFinished()).isFalse();
            when(timeSource.get()).thenReturn(NOW.plus(ELECTION_TIMEOUT.multipliedBy(4)));
            assertThat(addServer.isFinished()).isTrue();
            assertThat(addServer.getResponseFuture()).isCompletedWithValue(AddServerResponse.getTimeout());
        }

        @Test
        void willStopReplicatingToTheNewServer() {
            addServer.start();
            when(timeSource.get()).thenReturn(NOW.plus(ELECTION_TIMEOUT.multipliedBy(4)));
            verify(replicationManager, never()).stopReplicatingTo(SERVER_ID);
            assertThat(addServer.isFinished()).isTrue();
            verify(replicationManager).stopReplicatingTo(SERVER_ID);
        }

        @Test
        void timeoutsAreIdempotent() {
            addServer.start();
            when(timeSource.get()).thenReturn(NOW.plus(ELECTION_TIMEOUT.multipliedBy(4)));
            assertThat(addServer.isFinished()).isTrue();
            assertThat(addServer.isFinished()).isTrue();
            verify(replicationManager).stopReplicatingTo(SERVER_ID);
        }
    }

    @Nested
    class EntryCommitted {

        @Test
        void willResolveResponseFutureWhenConfigAppendIndexIsReached() {
            addServer.start();
            addServer.matchIndexAdvanced(SERVER_ID, FIRST_ROUND_END);
            addServer.matchIndexAdvanced(SERVER_ID, SECOND_ROUND_END);

            assertThat(addServer.getResponseFuture()).isNotCompleted();
            addServer.entryCommitted(APPEND_INDEX);
            assertThat(addServer.getResponseFuture()).isCompletedWithValue(AddServerResponse.getOK());
        }

        @Test
        void willNotResolveResponseFutureWhenConfigAppendIndexIsNotReached() {
            addServer.start();
            addServer.matchIndexAdvanced(SERVER_ID, FIRST_ROUND_END);
            addServer.matchIndexAdvanced(SERVER_ID, SECOND_ROUND_END);

            addServer.entryCommitted(APPEND_INDEX - 1);
            assertThat(addServer.getResponseFuture()).isNotCompleted();
        }
    }

    @Nested
    class Close {

        @Test
        void willCompleteFutureWithNotLeader() {
            addServer.close();
            assertThat(addServer.getResponseFuture()).isCompletedWithValue(AddServerResponse.getNotLeader());
        }
    }
}
