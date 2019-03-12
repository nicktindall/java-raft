package au.id.tindall.distalg.raft.rpc;

import static au.id.tindall.distalg.raft.SerializationUtils.roundTripSerializeDeserialize;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import au.id.tindall.distalg.raft.log.entries.LogEntry;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.StateMachineCommandEntry;
import org.junit.Test;

public class AppendEntriesRequestTest {

    private static final Term TERM_0 = new Term(0);
    private static final Term TERM_1 = new Term(1);
    private static final byte[] COMMAND_BYTES = "what".getBytes();

    @Test
    public void isSerializable() {
        assertThatCode(() -> roundTripSerializeDeserialize(new AppendEntriesRequest<>(TERM_1, 111L, 222L, 333, Optional.of(TERM_0), List.of(new StateMachineCommandEntry(TERM_0, COMMAND_BYTES)), 10)))
                .doesNotThrowAnyException();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void getEntries_WillReturnAnUnmodifiableList() {
        AppendEntriesRequest<Long> request = new AppendEntriesRequest<>(TERM_1, 111L, 222L, 333, Optional.of(TERM_0), List.of(new StateMachineCommandEntry(TERM_0, COMMAND_BYTES)), 10);
        request.getEntries().add(new StateMachineCommandEntry(TERM_0, COMMAND_BYTES));
    }

    @Test
    public void constructor_WillStoreACopyOfTheEntriesList() {
        ArrayList<LogEntry> originalEntriesList = new ArrayList<>();
        LogEntry e1 = new StateMachineCommandEntry(TERM_0, COMMAND_BYTES);
        originalEntriesList.add(e1);
        AppendEntriesRequest<Long> request = new AppendEntriesRequest<>(TERM_1, 111L, 222L, 333, Optional.of(TERM_0), originalEntriesList, 10);
        originalEntriesList.add(e1);
        assertThat(request.getEntries()).usingRecursiveFieldByFieldElementComparator().containsExactly(e1);
    }
}