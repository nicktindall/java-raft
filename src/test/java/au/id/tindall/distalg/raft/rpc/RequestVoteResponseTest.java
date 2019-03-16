package au.id.tindall.distalg.raft.rpc;

import static au.id.tindall.distalg.raft.SerializationUtils.roundTripSerializeDeserialize;
import static org.assertj.core.api.Assertions.assertThatCode;

import au.id.tindall.distalg.raft.log.Term;
import org.junit.jupiter.api.Test;

public class RequestVoteResponseTest {

    @Test
    public void isSerializable() {
        assertThatCode(() -> roundTripSerializeDeserialize(new RequestVoteResponse<>(new Term(12), 123L, 456L, true))).doesNotThrowAnyException();
    }
}