package au.id.tindall.distalg.raft.log;

import au.id.tindall.distalg.raft.log.entries.LogEntry;
import au.id.tindall.distalg.raft.log.entries.StateMachineCommandEntry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Optional;

import static au.id.tindall.distalg.raft.DomainUtils.logContaining;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@ExtendWith(MockitoExtension.class)
class LogTest {

    private static final Term TERM_0 = new Term(0);
    private static final Term TERM_1 = new Term(1);
    private static final int CLIENT_ID = 123;
    private static final LogEntry ENTRY_1 = new StateMachineCommandEntry(TERM_0, CLIENT_ID, -1, 0, "first".getBytes());
    private static final LogEntry ENTRY_2 = new StateMachineCommandEntry(TERM_0, CLIENT_ID, -1, 1, "second".getBytes());
    private static final LogEntry ENTRY_3 = new StateMachineCommandEntry(TERM_0, CLIENT_ID, -1, 2, "third".getBytes());
    private static final LogEntry ENTRY_4 = new StateMachineCommandEntry(TERM_0, CLIENT_ID, -1, 3, "fourth".getBytes());
    private static final LogEntry ENTRY_3B = new StateMachineCommandEntry(TERM_1, CLIENT_ID, -1, 2, "alt_third".getBytes());
    private static final LogEntry ENTRY_4B = new StateMachineCommandEntry(TERM_1, CLIENT_ID, -1, 3, "alt_fourth".getBytes());

    @Mock
    private EntryCommittedEventHandler entryCommittedEventHandler;

    @Nested
    class AppendEntries {

        @Mock
        private EntryAppendedEventHandler entryAppendedEventHandler;

        private Log log;

        @Nested
        class AtTheEndOfTheLog {

            @BeforeEach
            void setUp() {
                log = new Log();
                log.addEntryAppendedEventHandler(entryAppendedEventHandler);
                log.appendEntries(0, List.of(ENTRY_1, ENTRY_2, ENTRY_3));
            }

            @Test
            void willAddNewEntries() {
                assertThat(log.getEntries()).containsExactly(ENTRY_1, ENTRY_2, ENTRY_3);
            }

            @Test
            void willEmitEntryAppendedEvents() {
                final InOrder sequence = inOrder(entryAppendedEventHandler);
                sequence.verify(entryAppendedEventHandler).entryAppended(1, ENTRY_1);
                sequence.verify(entryAppendedEventHandler).entryAppended(2, ENTRY_2);
                sequence.verify(entryAppendedEventHandler).entryAppended(3, ENTRY_3);
                sequence.verifyNoMoreInteractions();
            }
        }

        @Nested
        class WhenTailDiffers {

            @BeforeEach
            void setUp() {
                log = logContaining(ENTRY_1, ENTRY_2, ENTRY_3, ENTRY_4);
                log.addEntryAppendedEventHandler(entryAppendedEventHandler);
                log.appendEntries(2, List.of(ENTRY_3B, ENTRY_4B));
            }

            @Test
            void willOverwriteTail() {
                assertThat(log.getEntries()).containsExactly(ENTRY_1, ENTRY_2, ENTRY_3B, ENTRY_4B);
            }

            @Test
            void willEmitEntryAppendedEvents() {
                final InOrder sequence = inOrder(entryAppendedEventHandler);
                sequence.verify(entryAppendedEventHandler).entryAppended(3, ENTRY_3B);
                sequence.verify(entryAppendedEventHandler).entryAppended(4, ENTRY_4B);
                sequence.verifyNoMoreInteractions();
            }
        }

        @Nested
        class WhenTailPartiallyDiffers {

            @BeforeEach
            void setUp() {
                log = logContaining(ENTRY_1, ENTRY_2, ENTRY_3, ENTRY_4);
                log.addEntryAppendedEventHandler(entryAppendedEventHandler);
                log.appendEntries(2, List.of(ENTRY_3, ENTRY_4B));
            }

            @Test
            void willOverwriteFromFirstDifference() {
                assertThat(log.getEntries()).containsExactly(ENTRY_1, ENTRY_2, ENTRY_3, ENTRY_4B);
            }

            @Test
            void willEmitEntryAppendedEventsFromFirstDifference() {
                verify(entryAppendedEventHandler).entryAppended(4, ENTRY_4B);
                verifyNoMoreInteractions(entryAppendedEventHandler);
            }
        }

        @Test
        void willStopNotifyingAppendedEventHandlerAfterItIsRemoved() {
            log = new Log();
            log.addEntryAppendedEventHandler(entryAppendedEventHandler);
            log.appendEntries(0, List.of(ENTRY_1, ENTRY_2));
            log.removeEntryAppendedEventHandler(entryAppendedEventHandler);
            log.appendEntries(2, List.of(ENTRY_3, ENTRY_4));
            verify(entryAppendedEventHandler).entryAppended(1, ENTRY_1);
            verify(entryAppendedEventHandler).entryAppended(2, ENTRY_2);
            verifyNoMoreInteractions(entryAppendedEventHandler);
        }

        @Test
        void willFail_WhenPrevLogIndexIsInvalid() {
            Log log = new Log();
            assertThatCode(
                    () -> log.appendEntries(-1, List.of(ENTRY_1, ENTRY_2))
            ).isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void willFail_WhenPrevLogIndexIsNotPresent() {
            Log log = new Log();
            assertThatCode(
                    () -> log.appendEntries(1, List.of(ENTRY_1, ENTRY_2))
            ).isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void willFail_WhenAttemptIsMadeToRewriteLogBeforeCommitIndex() {
            Log log = logContaining(ENTRY_1, ENTRY_2, ENTRY_3);
            log.advanceCommitIndex(3);
            assertThatThrownBy(() -> log.appendEntries(2, List.of(ENTRY_3B)))
                    .isInstanceOf(IllegalStateException.class);
        }
    }

    @Nested
    class GetEntries {

        @Test
        void returnsUnmodifiableList() {
            Log log = new Log();
            log.appendEntries(0, List.of(ENTRY_1, ENTRY_2, ENTRY_3));
            List<LogEntry> entries = log.getEntries();
            assertThatCode(
                    () -> entries.remove(0)
            ).isInstanceOf(UnsupportedOperationException.class);
        }

        @Test
        void returnsLimitedSubList() {
            Log log = new Log();
            log.appendEntries(0, List.of(ENTRY_1, ENTRY_2, ENTRY_3));
            List<LogEntry> entries = log.getEntries(1, 2);
            assertThat(entries).isEqualTo(List.of(ENTRY_1, ENTRY_2));
        }

        @Test
        void returnsAllAvailableUpToLimit() {
            Log log = new Log();
            log.appendEntries(0, List.of(ENTRY_1, ENTRY_2, ENTRY_3));
            List<LogEntry> entries = log.getEntries(2, 10);
            assertThat(entries).isEqualTo(List.of(ENTRY_2, ENTRY_3));
        }

        @Test
        void returnsUnmodifiableSubList() {
            Log log = new Log();
            log.appendEntries(0, List.of(ENTRY_1, ENTRY_2, ENTRY_3));
            List<LogEntry> entries = log.getEntries(2, 10);
            assertThatCode(
                    () -> entries.remove(0)
            ).isInstanceOf(UnsupportedOperationException.class);
        }
    }

    @Nested
    class ContainsPrevLogEntry {

        @Test
        void containsPrevLogEntry_WillReturnTrue_WhenAMatchingEntryIsFound() {
            assertThat(logContaining(ENTRY_1, ENTRY_2, ENTRY_3).containsPreviousEntry(1, TERM_0)).isTrue();
        }

        @Test
        void containsPrevLogEntry_WillReturnFalse_WhenTermsDoNotMatch() {
            assertThat(logContaining(ENTRY_1, ENTRY_2, ENTRY_3).containsPreviousEntry(1, TERM_1)).isFalse();
        }

        @Test
        void containsPrevLogEntry_WillReturnFalse_WhenPreviousIsNotPresent() {
            assertThat(logContaining(ENTRY_1, ENTRY_2, ENTRY_3).containsPreviousEntry(5, TERM_1)).isFalse();
        }
    }

    @Nested
    class GetLastLogIndex {

        @Test
        void willReturnIndexOfLastLogEntry() {
            assertThat(logContaining(ENTRY_1, ENTRY_2).getLastLogIndex()).isEqualTo(2);
        }

        @Test
        void willReturnZero_WhenLogIsEmpty() {
            assertThat(logContaining().getLastLogIndex()).isZero();
        }
    }

    @Nested
    class GetNextLogIndex {

        @Test
        void willReturnIndexOfNextLogEntry() {
            assertThat(logContaining(ENTRY_1, ENTRY_2).getNextLogIndex()).isEqualTo(3);
        }

        @Test
        void willReturnOne_WhenLogIsEmpty() {
            assertThat(logContaining().getNextLogIndex()).isEqualTo(1);
        }
    }

    @Nested
    class GetLastLogTerm {

        @Test
        void willReturnTermOfLastEntry() {
            assertThat(logContaining(ENTRY_1, ENTRY_2, ENTRY_3).getLastLogTerm()).contains(TERM_0);
        }

        @Test
        void willReturnEmptyWhenLogIsEmpty() {
            assertThat(logContaining().getLastLogTerm()).isEmpty();
        }
    }

    @Test
    void getSummary_WillReturnLastLogTermAndIndex() {
        assertThat(logContaining(ENTRY_1, ENTRY_2, ENTRY_3).getSummary()).isEqualTo(new LogSummary(Optional.of(TERM_0), 3));
    }

    @Test
    void commitIndex_WillBeInitializedToZero() {
        assertThat(logContaining(ENTRY_1, ENTRY_2, ENTRY_3, ENTRY_4).getCommitIndex()).isZero();
    }

    @Nested
    class UpdateCommitIndex {

        @Test
        void willUpdateCommitIndex_WhenMajorityOfOddNumberOfServersHaveAdvanced() {
            Log log = logContaining(ENTRY_1, ENTRY_2, ENTRY_3, ENTRY_4);
            Optional<Integer> newCommitIndex = log.updateCommitIndex(List.of(0, 0, 3, 4), TERM_0);
            assertThat(newCommitIndex).contains(3);
        }

        @Test
        void willUpdateCommitIndex_WhenMajorityOfEvenNumberOfServersHaveAdvanced() {
            Log log = logContaining(ENTRY_1, ENTRY_2, ENTRY_3, ENTRY_4);
            Optional<Integer> newCommitIndex = log.updateCommitIndex(List.of(0, 2, 3), TERM_0);
            assertThat(newCommitIndex).contains(2);
        }

        @Test
        void willNotUpdateCommitIndex_WhenMajorityOfOddNumberOfServersHaveNotAdvanced() {
            Log log = logContaining(ENTRY_1, ENTRY_2, ENTRY_3, ENTRY_4);
            Optional<Integer> newCommitIndex = log.updateCommitIndex(List.of(0, 0, 0, 4), TERM_0);
            assertThat(newCommitIndex).isEmpty();
        }

        @Test
        void willNotUpdateCommitIndex_WhenMajorityOfEvenNumberOfServersHaveNotAdvanced() {
            Log log = logContaining(ENTRY_1, ENTRY_2, ENTRY_3, ENTRY_4);
            Optional<Integer> newCommitIndex = log.updateCommitIndex(List.of(0, 0, 4), TERM_0);
            assertThat(newCommitIndex).isEmpty();
        }

        @Test
        void willNotifyEntryCommittedEventHandlers_WhenCommitIndexAdvances() {
            Log log = logContaining(ENTRY_1, ENTRY_2, ENTRY_3, ENTRY_4);
            log.addEntryCommittedEventHandler(entryCommittedEventHandler);
            log.updateCommitIndex(List.of(0, 0, 3, 4), TERM_0);
            InOrder sequence = inOrder(entryCommittedEventHandler);
            sequence.verify(entryCommittedEventHandler).entryCommitted(1, ENTRY_1);
            sequence.verify(entryCommittedEventHandler).entryCommitted(2, ENTRY_2);
            sequence.verify(entryCommittedEventHandler).entryCommitted(3, ENTRY_3);
        }

        @Test
        public void willNotAdvanceCommitIndexForEntriesFromPreviousTerms() {
            Log log = logContaining(ENTRY_1, ENTRY_2, ENTRY_3B, ENTRY_4B);
            log.addEntryCommittedEventHandler(entryCommittedEventHandler);
            log.updateCommitIndex(List.of(0, 0, 2, 2), TERM_1);
            verifyNoInteractions(entryCommittedEventHandler);
        }
    }

    @Test
    void removeEntryCommittedEventHandler_WillStopHandlerBeingNotified() {
        Log log = logContaining(ENTRY_1, ENTRY_2, ENTRY_3, ENTRY_4);
        log.addEntryCommittedEventHandler(entryCommittedEventHandler);
        log.updateCommitIndex(List.of(1, 1, 1), TERM_0);
        verify(entryCommittedEventHandler).entryCommitted(1, ENTRY_1);
        log.removeEntryCommittedEventHandler(entryCommittedEventHandler);
        log.updateCommitIndex(List.of(2, 2, 2), TERM_0);
        verifyNoMoreInteractions(entryCommittedEventHandler);
    }

    @Nested
    class AdvanceCommitIndex {

        @Test
        void willNotifyEntryCommittedEventHandlers_WhenCommitIndexAdvances() {
            Log log = logContaining(ENTRY_1, ENTRY_2);
            log.addEntryCommittedEventHandler(entryCommittedEventHandler);
            log.advanceCommitIndex(2);
            InOrder sequence = inOrder(entryCommittedEventHandler);
            sequence.verify(entryCommittedEventHandler).entryCommitted(1, ENTRY_1);
            sequence.verify(entryCommittedEventHandler).entryCommitted(2, ENTRY_2);
        }

        @Test
        void willNotNotifyCommittedEventHandlers_WhenCommitIndexDoesNotAdvance() {
            Log log = logContaining(ENTRY_1, ENTRY_2, ENTRY_3, ENTRY_4);
            log.advanceCommitIndex(4);
            log.addEntryCommittedEventHandler(entryCommittedEventHandler);
            log.advanceCommitIndex(4);
            verifyNoInteractions(entryCommittedEventHandler);
        }

        @Test
        void willIgnore_WhenCommitIndexGoesBackwards() {
            Log log = logContaining(ENTRY_1, ENTRY_2, ENTRY_3, ENTRY_4);
            log.advanceCommitIndex(4);
            log.addEntryCommittedEventHandler(entryCommittedEventHandler);
            log.advanceCommitIndex(3);
            verifyNoInteractions(entryCommittedEventHandler);
            assertThat(log.getCommitIndex()).isEqualTo(4);
        }
    }
}