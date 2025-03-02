package au.id.tindall.distalg.raft.rpc.server;

import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.serialisation.LongIDSerializer;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static au.id.tindall.distalg.raft.SerializationUtils.roundTripSerializeDeserialize;
import static org.assertj.core.api.Assertions.assertThat;

class RequestVoteRequestTest {

    @Test
    void isStreamable() {
        RequestVoteRequest<Long> request = new RequestVoteRequest<>(new Term(16), 12345L, 456, Optional.of(new Term(5)), false);
        assertThat(roundTripSerializeDeserialize(request, LongIDSerializer.INSTANCE))
                .usingRecursiveComparison()
                .isEqualTo(request);
    }
}