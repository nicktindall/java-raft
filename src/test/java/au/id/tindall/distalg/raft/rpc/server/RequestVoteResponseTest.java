package au.id.tindall.distalg.raft.rpc.server;

import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.serialisation.LongIDSerializer;
import org.junit.jupiter.api.Test;

import static au.id.tindall.distalg.raft.SerializationUtils.roundTripSerializeDeserialize;
import static org.assertj.core.api.Assertions.assertThat;

class RequestVoteResponseTest {

    @Test
    void isStreamable() {
        RequestVoteResponse<Long> response = new RequestVoteResponse<>(new Term(12), 123L, true);
        assertThat(roundTripSerializeDeserialize(response, LongIDSerializer.INSTANCE))
                .usingRecursiveComparison()
                .isEqualTo(response);
    }
}