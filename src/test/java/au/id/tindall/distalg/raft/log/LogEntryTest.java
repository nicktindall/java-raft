package au.id.tindall.distalg.raft.log;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;

import org.junit.Test;

public class LogEntryTest {

    private static final Term TERM_1 = new Term(1);

    @Test
    public void constructor_WillCopyCommandBytes() {
        byte[] originalBytes = "something".getBytes();
        LogEntry logEntry = new LogEntry(TERM_1, originalBytes);
        Arrays.fill(originalBytes, (byte) 0);
        assertThat(logEntry.getCommand()).isEqualTo("something".getBytes());
    }

    @Test
    public void getCommand_WillReturnCopyOfCommandBytes() {
        LogEntry logEntry = new LogEntry(TERM_1, "something".getBytes());
        byte[] commandBytes = logEntry.getCommand();
        Arrays.fill(commandBytes, (byte) 0);
        assertThat(logEntry.getCommand()).isEqualTo("something".getBytes());
    }
}