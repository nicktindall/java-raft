package au.id.tindall.distalg.raft.rpc.server;

import au.id.tindall.distalg.raft.log.Term;
import org.junit.jupiter.api.Test;

import static au.id.tindall.distalg.raft.SerializationUtils.roundTripSerializeDeserialize;
import static org.assertj.core.api.Assertions.assertThatCode;

class RequestVoteResponseTest {

    @Test
    void isSerializable() {
        assertThatCode(() -> roundTripSerializeDeserialize(new RequestVoteResponse<>(new Term(12), 123L, true))).doesNotThrowAnyException();
    }
}