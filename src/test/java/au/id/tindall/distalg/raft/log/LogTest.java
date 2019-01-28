package au.id.tindall.distalg.raft.log;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

public class LogTest {

    private static final Term TERM_0 = new Term(0);
    private static final Term TERM_1 = new Term(1);
    private static final Term TERM_2 = new Term(2);
    private static final LogEntry ENTRY_1 = new LogEntry(TERM_0, "first".getBytes());
    private static final LogEntry ENTRY_2 = new LogEntry(TERM_0, "second".getBytes());
    private static final LogEntry ENTRY_3 = new LogEntry(TERM_1, "third".getBytes());
    private static final LogEntry ENTRY_4 = new LogEntry(TERM_1, "fourth".getBytes());
    private static final LogEntry ENTRY_3B = new LogEntry(TERM_2, "alt_third".getBytes());
    private static final LogEntry ENTRY_4B = new LogEntry(TERM_2, "alt_fourth".getBytes());

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

    @Test(expected = IllegalArgumentException.class)
    public void appendEntries_WillFail_WhenPrevLogIndexIsInvalid() {
        Log log = new Log();
        log.appendEntries(-1, List.of(ENTRY_1, ENTRY_2));
    }

    @Test(expected = IllegalArgumentException.class)
    public void appendEntries_WillFail_WhenPrevLogIndexIsNotPresent() {
        Log log = new Log();
        log.appendEntries(1, List.of(ENTRY_1, ENTRY_2));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void getEntries_ReturnsUnmodifiableList() {
        Log log = new Log();
        log.appendEntries(0, List.of(ENTRY_1, ENTRY_2, ENTRY_3));
        List<LogEntry> entries = log.getEntries();
        entries.remove(0);
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

    private Log logContaining(LogEntry... entries) {
        Log log = new Log();
        log.appendEntries(0, Arrays.asList(entries));
        return log;
    }
}