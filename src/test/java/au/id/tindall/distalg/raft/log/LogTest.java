package au.id.tindall.distalg.raft.log;

import static au.id.tindall.distalg.raft.DomainUtils.logContaining;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verifyZeroInteractions;

import java.util.List;
import java.util.Optional;

import au.id.tindall.distalg.raft.log.entries.LogEntry;
import au.id.tindall.distalg.raft.log.entries.StateMachineCommandEntry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class LogTest {

    private static final Term TERM_0 = new Term(0);
    private static final Term TERM_1 = new Term(1);
    private static final Term TERM_2 = new Term(2);
    private static final LogEntry ENTRY_1 = new StateMachineCommandEntry(TERM_0, "first".getBytes());
    private static final LogEntry ENTRY_2 = new StateMachineCommandEntry(TERM_0, "second".getBytes());
    private static final LogEntry ENTRY_3 = new StateMachineCommandEntry(TERM_1, "third".getBytes());
    private static final LogEntry ENTRY_4 = new StateMachineCommandEntry(TERM_1, "fourth".getBytes());
    private static final LogEntry ENTRY_3B = new StateMachineCommandEntry(TERM_2, "alt_third".getBytes());
    private static final LogEntry ENTRY_4B = new StateMachineCommandEntry(TERM_2, "alt_fourth".getBytes());

    @Mock
    private EntryCommittedEventHandler entryCommittedEventHandler;

    @Test
    public void appendEntries_WillAddNewEntriesToTheEndOfTheLog() {
        Log log = new Log();
        log.appendEntries(0, List.of(ENTRY_1, ENTRY_2, ENTRY_3));
        assertThat(log.getEntries()).containsExactly(ENTRY_1, ENTRY_2, ENTRY_3);
    }

    @Test
    public void appendEntries_WillOverwriteTailWhenItDiffers() {
        Log log = logContaining(ENTRY_1, ENTRY_2, ENTRY_3, ENTRY_4);
        log.appendEntries(2, List.of(ENTRY_3B, ENTRY_4B));
        assertThat(log.getEntries()).containsExactly(ENTRY_1, ENTRY_2, ENTRY_3B, ENTRY_4B);
    }

    @Test
    public void appendEntries_WillOverwriteTailWhenItPartiallyDiffers() {
        Log log = logContaining(ENTRY_1, ENTRY_2, ENTRY_3, ENTRY_4);
        log.appendEntries(2, List.of(ENTRY_3, ENTRY_4B));
        assertThat(log.getEntries()).containsExactly(ENTRY_1, ENTRY_2, ENTRY_3, ENTRY_4B);
    }

    @Test
    public void appendEntries_WillFail_WhenPrevLogIndexIsInvalid() {
        Log log = new Log();
        assertThatCode(
                () -> log.appendEntries(-1, List.of(ENTRY_1, ENTRY_2))
        ).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void appendEntries_WillFail_WhenPrevLogIndexIsNotPresent() {
        Log log = new Log();
        assertThatCode(
                () -> log.appendEntries(1, List.of(ENTRY_1, ENTRY_2))
        ).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void getEntries_ReturnsUnmodifiableList() {
        Log log = new Log();
        log.appendEntries(0, List.of(ENTRY_1, ENTRY_2, ENTRY_3));
        List<LogEntry> entries = log.getEntries();
        assertThatCode(
                () -> entries.remove(0)
        ).isInstanceOf(UnsupportedOperationException.class);
    }

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

    @Test
    public void getLastLogIndex_WillReturnIndexOfLastLogEntry() {
        assertThat(logContaining(ENTRY_1, ENTRY_2).getLastLogIndex()).isEqualTo(2);
    }

    @Test
    public void getLastLogIndex_WillReturnZero_WhenLogIsEmpty() {
        assertThat(logContaining().getLastLogIndex()).isZero();
    }

    @Test
    public void getLastLogTerm_WillReturnTermOfLastEntry() {
        assertThat(logContaining(ENTRY_1, ENTRY_2, ENTRY_3).getLastLogTerm()).contains(TERM_1);
    }

    @Test
    public void getLastLogTerm_WillReturnEmptyWhenLogIsEmpty() {
        assertThat(logContaining().getLastLogTerm()).isEmpty();
    }

    @Test
    public void getSummary_WillReturnLastLogTermAndIndex() {
        assertThat(logContaining(ENTRY_1, ENTRY_2, ENTRY_3).getSummary()).isEqualTo(new LogSummary(Optional.of(TERM_1), 3));
    }

    @Test
    public void commitIndex_WillBeInitializedToZero() {
        assertThat(logContaining(ENTRY_1, ENTRY_2, ENTRY_3, ENTRY_4).getCommitIndex()).isZero();
    }

    @Test
    public void updateCommitIndex_WillUpdateCommitIndex_WhenMajorityOfOddNumberOfServersHaveAdvanced() {
        Log log = logContaining(ENTRY_1, ENTRY_2, ENTRY_3, ENTRY_4);
        log.updateCommitIndex(List.of(0, 0, 3, 4));
        assertThat(log.getCommitIndex()).isEqualTo(3);
    }

    @Test
    public void updateCommitIndex_WillUpdateCommitIndex_WhenMajorityOfEvenNumberOfServersHaveAdvanced() {
        Log log = logContaining(ENTRY_1, ENTRY_2, ENTRY_3, ENTRY_4);
        log.updateCommitIndex(List.of(0, 2, 3));
        assertThat(log.getCommitIndex()).isEqualTo(2);
    }

    @Test
    public void updateCommitIndex_WillNotUpdateCommitIndex_WhenMajorityOfOddNumberOfServersHaveNotAdvanced() {
        Log log = logContaining(ENTRY_1, ENTRY_2, ENTRY_3, ENTRY_4);
        log.updateCommitIndex(List.of(0, 0, 0, 4));
        assertThat(log.getCommitIndex()).isEqualTo(0);
    }

    @Test
    public void updateCommitIndex_WillNotUpdateCommitIndex_WhenMajorityOfEvenNumberOfServersHaveNotAdvanced() {
        Log log = logContaining(ENTRY_1, ENTRY_2, ENTRY_3, ENTRY_4);
        log.updateCommitIndex(List.of(0, 0, 4));
        assertThat(log.getCommitIndex()).isEqualTo(0);
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

    @Test
    public void setCommitIndex_WillNotifyEntryCommittedEventHandlers_WhenCommitIndexAdvances() {
        Log log = logContaining(ENTRY_1, ENTRY_2);
        log.addEntryCommittedEventHandler(entryCommittedEventHandler);
        log.setCommitIndex(2);
        InOrder sequence = inOrder(entryCommittedEventHandler);
        sequence.verify(entryCommittedEventHandler).entryCommitted(1, ENTRY_1);
        sequence.verify(entryCommittedEventHandler).entryCommitted(2, ENTRY_2);
    }

    @Test
    public void setCommitIndex_WillNotNotifyEntryCommittedEventHandlers_WhenCommitIndexRecedes() {
        Log log = logContaining(ENTRY_1, ENTRY_2, ENTRY_3, ENTRY_4);
        log.setCommitIndex(4);
        log.addEntryCommittedEventHandler(entryCommittedEventHandler);
        log.setCommitIndex(2);
        verifyZeroInteractions(entryCommittedEventHandler);
    }
}