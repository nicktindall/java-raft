package au.id.tindall.distalg.raft.comms.netty.simplemesh.messages;

import au.id.tindall.distalg.raft.SerializationUtils;
import au.id.tindall.distalg.raft.serialisation.IntegerIDSerializer;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PeerDetailsTest {

    @Test
    void isNewerThanComparesNodeAndVersionTimestamp() {
        assertTrue(peerDetailsWithVersions(11, 11)
                .isNewerThan(peerDetailsWithVersions(10, 10)));
        assertTrue(peerDetailsWithVersions(10, 11)
                .isNewerThan(peerDetailsWithVersions(10, 10)));
        assertFalse(peerDetailsWithVersions(10, 10)
                .isNewerThan(peerDetailsWithVersions(10, 10)));
        assertFalse(peerDetailsWithVersions(10, 10)
                .isNewerThan(peerDetailsWithVersions(10, 11)));
        assertFalse(peerDetailsWithVersions(10, 10)
                .isNewerThan(peerDetailsWithVersions(11, 11)));
    }

    @Test
    void canSerializeAndDeserialize() {
        PeerDetails peerDetails = new PeerDetails(1, 10, 20, List.of("test.com:1234", "other.com:1227"));
        assertThat(SerializationUtils.roundTripSerializeDeserialize(peerDetails, IntegerIDSerializer.INSTANCE))
                .usingRecursiveComparison().isEqualTo(peerDetails);
    }

    private static PeerDetails peerDetailsWithVersions(long nodeTimestamp, long detailsTimestamp) {
        return new PeerDetails(1, nodeTimestamp, detailsTimestamp, List.of());
    }
}