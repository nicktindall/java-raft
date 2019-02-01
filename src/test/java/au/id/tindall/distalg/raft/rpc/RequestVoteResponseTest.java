package au.id.tindall.distalg.raft.rpc;

import static au.id.tindall.distalg.raft.SerializationUtils.roundTripSerializeDeserialize;
import static org.assertj.core.api.Assertions.assertThatCode;

import au.id.tindall.distalg.raft.log.Term;
import org.junit.Test;

public class RequestVoteResponseTest {

    @Test
    public void isSerializable() {
        assertThatCode(() -> roundTripSerializeDeserialize(new RequestVoteResponse<>(123L, new Term(12), true))).doesNotThrowAnyException();
    }
}