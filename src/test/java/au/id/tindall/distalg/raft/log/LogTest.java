package au.id.tindall.distalg.raft.log;

import au.id.tindall.distalg.raft.log.entries.LogEntry;
import au.id.tindall.distalg.raft.log.entries.StateMachineCommandEntry;
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
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;

@ExtendWith(MockitoExtension.class)
public class LogTest {

    private static final Term TERM_0 = new Term(0);
    private static final Term TERM_1 = new Term(1);
    private static final Term TERM_2 = new Term(2);
    private static final int CLIENT_ID = 123;
    private static final LogEntry ENTRY_1 = new StateMachineCommandEntry(TERM_0, CLIENT_ID, 0, "first".getBytes());
    private static final LogEntry ENTRY_2 = new StateMachineCommandEntry(TERM_0, CLIENT_ID, 1, "second".getBytes());
    private static final LogEntry ENTRY_3 = new StateMachineCommandEntry(TERM_1, CLIENT_ID, 2, "third".getBytes());
    private static final LogEntry ENTRY_4 = new StateMachineCommandEntry(TERM_1, CLIENT_ID, 3, "fourth".getBytes());
    private static final LogEntry ENTRY_3B = new StateMachineCommandEntry(TERM_2, CLIENT_ID, 2, "alt_third".getBytes());
    private static final LogEntry ENTRY_4B = new StateMachineCommandEntry(TERM_2, CLIENT_ID, 3, "alt_fourth".getBytes());

    @Mock
    private EntryCommittedEventHandler entryCommittedEventHandler;

    @Nested
    class AppendEntries {

        @Test
        public void willAddNewEntriesToTheEndOfTheLog() {
            Log log = new Log();
            log.appendEntries(0, List.of(ENTRY_1, ENTRY_2, ENTRY_3));
            assertThat(log.getEntries()).containsExactly(ENTRY_1, ENTRY_2, ENTRY_3);
        }

        @Test
        public void willOverwriteTail_WhenItDiffers() {
            Log log = logContaining(ENTRY_1, ENTRY_2, ENTRY_3, ENTRY_4);
            log.appendEntries(2, List.of(ENTRY_3B, ENTRY_4B));
            assertThat(log.getEntries()).containsExactly(ENTRY_1, ENTRY_2, ENTRY_3B, ENTRY_4B);
        }

        @Test
        public void willOverwriteTail_WhenItPartiallyDiffers() {
            Log log = logContaining(ENTRY_1, ENTRY_2, ENTRY_3, ENTRY_4);
            log.appendEntries(2, List.of(ENTRY_3, ENTRY_4B));
            assertThat(log.getEntries()).containsExactly(ENTRY_1, ENTRY_2, ENTRY_3, ENTRY_4B);
        }

        @Test
        public void willFail_WhenPrevLogIndexIsInvalid() {
            Log log = new Log();
            assertThatCode(
                    () -> log.appendEntries(-1, List.of(ENTRY_1, ENTRY_2))
            ).isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        public void willFail_WhenPrevLogIndexIsNotPresent() {
            Log log = new Log();
            assertThatCode(
                    () -> log.appendEntries(1, List.of(ENTRY_1, ENTRY_2))
            ).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Nested
    class GetEntries {

        @Test
        public void returnsUnmodifiableList() {
            Log log = new Log();
            log.appendEntries(0, List.of(ENTRY_1, ENTRY_2, ENTRY_3));
            List<LogEntry> entries = log.getEntries();
            assertThatCode(
                    () -> entries.remove(0)
            ).isInstanceOf(UnsupportedOperationException.class);
        }
    }

    @Nested
    class ContainsPrevLogEntry {

        @Test
        public void containsPrevLogEntry_WillReturnTrue_WhenAMatchingEntryIsFound() {
            assertThat(logContaining(ENTRY_1, ENTRY_2, ENTRY_3).containsPreviousEntry(1, TERM_0)).isTrue();
        }

        @Test
        public void containsPrevLogEntry_WillReturnFalse_WhenTermsDoNotMatch() {
            assertThat(logContaining(ENTRY_1, ENTRY_2, ENTRY_3).containsPreviousEntry(1, TERM_1)).isFalse();
        }

        @Test
        public void containsPrevLogEntry_WillReturnFalse_WhenPreviousIsNotPresent() {
            assertThat(logContaining(ENTRY_1, ENTRY_2, ENTRY_3).containsPreviousEntry(5, TERM_1)).isFalse();
        }
    }

    @Nested
    class GetLastLogIndex {

        @Test
        public void willReturnIndexOfLastLogEntry() {
            assertThat(logContaining(ENTRY_1, ENTRY_2).getLastLogIndex()).isEqualTo(2);
        }

        @Test
        public void willReturnZero_WhenLogIsEmpty() {
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
        public void willReturnTermOfLastEntry() {
            assertThat(logContaining(ENTRY_1, ENTRY_2, ENTRY_3).getLastLogTerm()).contains(TERM_1);
        }

        @Test
        public void willReturnEmptyWhenLogIsEmpty() {
            assertThat(logContaining().getLastLogTerm()).isEmpty();
        }
    }

    @Test
    public void getSummary_WillReturnLastLogTermAndIndex() {
        assertThat(logContaining(ENTRY_1, ENTRY_2, ENTRY_3).getSummary()).isEqualTo(new LogSummary(Optional.of(TERM_1), 3));
    }

    @Test
    public void commitIndex_WillBeInitializedToZero() {
        assertThat(logContaining(ENTRY_1, ENTRY_2, ENTRY_3, ENTRY_4).getCommitIndex()).isZero();
    }

    @Nested
    class UpdateCommitIndex {

        @Test
        public void updateCommitIndex_WillUpdateCommitIndex_WhenMajorityOfOddNumberOfServersHaveAdvanced() {
            Log log = logContaining(ENTRY_1, ENTRY_2, ENTRY_3, ENTRY_4);
            Optional<Integer> newCommitIndex = log.updateCommitIndex(List.of(0, 0, 3, 4));
            assertThat(newCommitIndex).contains(3);
        }

        @Test
        public void updateCommitIndex_WillUpdateCommitIndex_WhenMajorityOfEvenNumberOfServersHaveAdvanced() {
            Log log = logContaining(ENTRY_1, ENTRY_2, ENTRY_3, ENTRY_4);
            Optional<Integer> newCommitIndex = log.updateCommitIndex(List.of(0, 2, 3));
            assertThat(newCommitIndex).contains(2);
        }

        @Test
        public void updateCommitIndex_WillNotUpdateCommitIndex_WhenMajorityOfOddNumberOfServersHaveNotAdvanced() {
            Log log = logContaining(ENTRY_1, ENTRY_2, ENTRY_3, ENTRY_4);
            Optional<Integer> newCommitIndex = log.updateCommitIndex(List.of(0, 0, 0, 4));
            assertThat(newCommitIndex).isEmpty();
        }

        @Test
        public void updateCommitIndex_WillNotUpdateCommitIndex_WhenMajorityOfEvenNumberOfServersHaveNotAdvanced() {
            Log log = logContaining(ENTRY_1, ENTRY_2, ENTRY_3, ENTRY_4);
            Optional<Integer> newCommitIndex = log.updateCommitIndex(List.of(0, 0, 4));
            assertThat(newCommitIndex).isEmpty();
        }

        @Test
        public void updateCommitIndex_WillNotifyEntryCommittedEventHandlers_WhenCommitIndexAdvances() {
            Log log = logContaining(ENTRY_1, ENTRY_2, ENTRY_3, ENTRY_4);
            log.addEntryCommittedEventHandler(entryCommittedEventHandler);
            log.updateCommitIndex(List.of(0, 0, 3, 4));
            InOrder sequence = inOrder(entryCommittedEventHandler);
            sequence.verify(entryCommittedEventHandler).entryCommitted(1, ENTRY_1);
            sequence.verify(entryCommittedEventHandler).entryCommitted(2, ENTRY_2);
            sequence.verify(entryCommittedEventHandler).entryCommitted(3, ENTRY_3);
        }
    }

    @Test
    void removeEntryCommittedEventHandler_WillStopHandlerBeingNotified() {
        Log log = logContaining(ENTRY_1, ENTRY_2, ENTRY_3, ENTRY_4);
        log.addEntryCommittedEventHandler(entryCommittedEventHandler);
        log.updateCommitIndex(List.of(1, 1, 1));
        verify(entryCommittedEventHandler).entryCommitted(1, ENTRY_1);
        log.removeEntryCommittedEventHandler(entryCommittedEventHandler);
        log.updateCommitIndex(List.of(2, 2, 2));
        verifyNoMoreInteractions(entryCommittedEventHandler);
    }

    @Nested
    class SetCommitIndex {

        @Test
        public void willNotifyEntryCommittedEventHandlers_WhenCommitIndexAdvances() {
            Log log = logContaining(ENTRY_1, ENTRY_2);
            log.addEntryCommittedEventHandler(entryCommittedEventHandler);
            log.setCommitIndex(2);
            InOrder sequence = inOrder(entryCommittedEventHandler);
            sequence.verify(entryCommittedEventHandler).entryCommitted(1, ENTRY_1);
            sequence.verify(entryCommittedEventHandler).entryCommitted(2, ENTRY_2);
        }

        @Test
        public void willNotNotifyEntryCommittedEventHandlers_WhenCommitIndexRecedes() {
            Log log = logContaining(ENTRY_1, ENTRY_2, ENTRY_3, ENTRY_4);
            log.setCommitIndex(4);
            log.addEntryCommittedEventHandler(entryCommittedEventHandler);
            log.setCommitIndex(2);
            verifyZeroInteractions(entryCommittedEventHandler);
        }
    }
}