package au.id.tindall.distalg.raft.rpc.server;

import static au.id.tindall.distalg.raft.SerializationUtils.roundTripSerializeDeserialize;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.util.Optional;

import au.id.tindall.distalg.raft.log.Term;
import org.junit.jupiter.api.Test;

public class RequestVoteRequestTest {

    @Test
    public void isSerializable() {
        assertThatCode(() -> roundTripSerializeDeserialize(new RequestVoteRequest<>(new Term(16), 12345L, 456, Optional.of(new Term(5))))).doesNotThrowAnyException();
    }
}