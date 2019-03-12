package au.id.tindall.distalg.raft.log;

import static au.id.tindall.distalg.raft.SerializationUtils.roundTripSerializeDeserialize;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.util.Arrays;

import au.id.tindall.distalg.raft.log.entries.StateMachineCommandEntry;
import org.junit.Test;

public class StateMachineCommandEntryTest {

    private static final Term TERM_1 = new Term(1);

    @Test
    public void isSerializable() {
        assertThatCode(() -> roundTripSerializeDeserialize(new StateMachineCommandEntry(new Term(2), "command bytes".getBytes()))).doesNotThrowAnyException();
    }

    @Test
    public void constructor_WillCopyCommandBytes() {
        byte[] originalBytes = "something".getBytes();
        StateMachineCommandEntry logEntry = new StateMachineCommandEntry(TERM_1, originalBytes);
        Arrays.fill(originalBytes, (byte) 0);
        assertThat(logEntry.getCommand()).isEqualTo("something".getBytes());
    }

    @Test
    public void getCommand_WillReturnCopyOfCommandBytes() {
        StateMachineCommandEntry logEntry = new StateMachineCommandEntry(TERM_1, "something".getBytes());
        byte[] commandBytes = logEntry.getCommand();
        Arrays.fill(commandBytes, (byte) 0);
        assertThat(logEntry.getCommand()).isEqualTo("something".getBytes());
    }
}