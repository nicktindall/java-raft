package au.id.tindall.distalg.raft.rpc.server;

import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.LogEntry;
import au.id.tindall.distalg.raft.log.entries.StateMachineCommandEntry;
import au.id.tindall.distalg.raft.serialisation.LongIDSerializer;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static au.id.tindall.distalg.raft.SerializationUtils.roundTripSerializeDeserialize;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

class AppendEntriesRequestTest {

    private static final Term TERM_0 = new Term(0);
    private static final Term TERM_1 = new Term(1);
    private static final byte[] COMMAND_BYTES = "what".getBytes();

    @Test
    void isStreamable() {
        AppendEntriesRequest<Long> request = new AppendEntriesRequest<>(TERM_1, 111L, 333, Optional.of(TERM_0), List.of(new StateMachineCommandEntry(TERM_0, 444, -1, 0, COMMAND_BYTES)), 10);
        assertThat(roundTripSerializeDeserialize(request, LongIDSerializer.INSTANCE))
                .usingRecursiveComparison().isEqualTo(request);
    }

    @Test
    void getEntries_WillReturnAnUnmodifiableList() {
        AppendEntriesRequest<Long> request = new AppendEntriesRequest<>(TERM_1, 111L, 333, Optional.of(TERM_0), List.of(new StateMachineCommandEntry(TERM_0, 444, -1, 0, COMMAND_BYTES)), 10);
        assertThatCode(
                () -> request.getEntries().add(new StateMachineCommandEntry(TERM_0, 444, -1, 0, COMMAND_BYTES))
        ).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void constructor_WillStoreACopyOfTheEntriesList() {
        ArrayList<LogEntry> originalEntriesList = new ArrayList<>();
        LogEntry e1 = new StateMachineCommandEntry(TERM_0, 444, -1, 0, COMMAND_BYTES);
        originalEntriesList.add(e1);
        AppendEntriesRequest<Long> request = new AppendEntriesRequest<>(TERM_1, 111L, 333, Optional.of(TERM_0), originalEntriesList, 10);
        originalEntriesList.add(e1);
        assertThat(request.getEntries()).usingRecursiveFieldByFieldElementComparator().containsExactly(e1);
    }
}