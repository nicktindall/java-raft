package au.id.tindall.distalg.raft.rpc.server;

import au.id.tindall.distalg.raft.log.Term;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class AppendEntriesResponseTest {

    @Test
    void willReturnAppendedIndexWhenPresent() {
        assertThat(new AppendEntriesResponse<>(new Term(0), 100L, true, Optional.of(12)).getAppendedIndex()).contains(12);
    }

    @Test
    void willReturnEmptyWhenAppendedIndexIsAbsent() {
        assertThat(new AppendEntriesResponse<>(new Term(0), 100L, true, Optional.empty()).getAppendedIndex()).isEmpty();
    }
}