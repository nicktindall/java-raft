package au.id.tindall.distalg.raft.rpc.client;

import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.ClientRegistrationEntry;
import au.id.tindall.distalg.raft.log.entries.ConfigurationEntry;
import au.id.tindall.distalg.raft.log.entries.LogEntry;
import au.id.tindall.distalg.raft.log.entries.StateMachineCommandEntry;
import au.id.tindall.distalg.raft.rpc.clustermembership.AddServerRequest;
import au.id.tindall.distalg.raft.rpc.clustermembership.AddServerResponse;
import au.id.tindall.distalg.raft.rpc.clustermembership.RemoveServerRequest;
import au.id.tindall.distalg.raft.rpc.clustermembership.RemoveServerResponse;
import au.id.tindall.distalg.raft.rpc.server.AppendEntriesRequest;
import au.id.tindall.distalg.raft.rpc.server.AppendEntriesResponse;
import au.id.tindall.distalg.raft.rpc.server.RequestVoteRequest;
import au.id.tindall.distalg.raft.rpc.server.RequestVoteResponse;
import au.id.tindall.distalg.raft.rpc.server.TimeoutNowMessage;
import au.id.tindall.distalg.raft.rpc.snapshots.InstallSnapshotRequest;
import au.id.tindall.distalg.raft.rpc.snapshots.InstallSnapshotResponse;
import au.id.tindall.distalg.raft.serialisation.ByteBufferIO;
import au.id.tindall.distalg.raft.serialisation.IntegerIDSerializer;
import au.id.tindall.distalg.raft.serialisation.Streamable;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import static au.id.tindall.distalg.raft.util.RandomTestUtil.randomBoolean;
import static au.id.tindall.distalg.raft.util.RandomTestUtil.randomByteArray;
import static au.id.tindall.distalg.raft.util.RandomTestUtil.randomEnum;
import static au.id.tindall.distalg.raft.util.RandomTestUtil.randomIntValue;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

class MessageSerializationTest {

    @Nested
    class Client {
        @Test
        void testClientRequestRequest() {
            testRoundTrip(new ClientRequestRequest<>(123, 456, 789, randomByteArray()));
        }

        @Test
        void testClientRequestResponse() {
            testRoundTrip(new ClientRequestResponse<>(randomEnum(ClientRequestStatus.class),
                    randomByteArray(),
                    null));
            testRoundTrip(new ClientRequestResponse<>(randomEnum(ClientRequestStatus.class),
                    randomByteArray(),
                    randomIntValue()));
        }

        @Test
        void testRegisterClientRequest() {
            testRoundTrip(new RegisterClientRequest<>());
        }

        @Test
        void testRegisterClientResponse() {
            testRoundTrip(new RegisterClientResponse<>(randomEnum(RegisterClientStatus.class), randomIntValue(), randomIntValue()));
            testRoundTrip(new RegisterClientResponse<>(randomEnum(RegisterClientStatus.class), null, null));
            testRoundTrip(new RegisterClientResponse<>(randomEnum(RegisterClientStatus.class), null, randomIntValue()));
        }
    }

    @Nested
    class AdminClient {
        @Test
        void testAddServerRequest() {
            testRoundTrip(new AddServerRequest<>(randomIntValue()));
        }

        @Test
        void testAddServerResponse() {
            testRoundTrip(AddServerResponse.OK);
            testRoundTrip(AddServerResponse.NOT_LEADER);
            testRoundTrip(AddServerResponse.TIMEOUT);
        }

        @Test
        void testRemoveServerRequest() {
            testRoundTrip(new RemoveServerRequest<>(randomIntValue()));
        }

        @Test
        void testRemoveServerResponse() {
            testRoundTrip(RemoveServerResponse.OK);
            testRoundTrip(RemoveServerResponse.NOT_LEADER);
        }
    }

    @Nested
    class RPC {

        @Test
        void testAppendEntriesRequest() {
            testRoundTrip(new AppendEntriesRequest<>(new Term(randomIntValue()), randomIntValue(), randomIntValue(), Optional.of(new Term(randomIntValue())),
                    randomLogEntries(), randomIntValue()));
            testRoundTrip(new AppendEntriesRequest<>(new Term(randomIntValue()), randomIntValue(), randomIntValue(), Optional.empty(),
                    randomLogEntries(), randomIntValue()));
        }

        @Test
        void testAppendEntriesResponse() {
            testRoundTrip(new AppendEntriesResponse<>(new Term(randomIntValue()), randomIntValue(), randomBoolean(), Optional.of(randomIntValue())));
            testRoundTrip(new AppendEntriesResponse<>(new Term(randomIntValue()), randomIntValue(), randomBoolean(), Optional.empty()));
        }

        @Test
        void testRequestVoteRequest() {
            testRoundTrip(new RequestVoteRequest<>(new Term(randomIntValue()), randomIntValue(), randomIntValue(), Optional.of(new Term(randomIntValue())), randomBoolean()));
            testRoundTrip(new RequestVoteRequest<>(new Term(randomIntValue()), randomIntValue(), randomIntValue(), Optional.empty(), randomBoolean()));
        }

        @Test
        void testRequestVoteResponse() {
            testRoundTrip(new RequestVoteResponse<>(new Term(randomIntValue()), randomIntValue(), randomBoolean()));
        }

        @Test
        void testInstallSnapshotRequest() {
            testRoundTrip(new InstallSnapshotRequest<>(new Term(randomIntValue()), randomIntValue(), randomIntValue(), new Term(randomIntValue()), randomConfigurationEntry(),
                    randomIntValue(), randomIntValue(), randomByteArray(), randomBoolean()));
        }

        @Test
        void testInstallSnapshotResponse() {
            testRoundTrip(new InstallSnapshotResponse<>(new Term(randomIntValue()), randomIntValue(), randomBoolean(), randomIntValue(), randomIntValue()));
        }

        @Test
        void testTimeoutNowRequest() {
            testRoundTrip(new TimeoutNowMessage<>(new Term(randomIntValue()), randomIntValue()));
            testRoundTrip(new TimeoutNowMessage<>(new Term(randomIntValue()), randomIntValue(), randomBoolean()));
        }
    }

    private List<LogEntry> randomLogEntries() {
        int numEntries = ThreadLocalRandom.current().nextInt(20);

        final List<LogEntry> entries = new ArrayList<>(numEntries);
        for (int i = 0; i < numEntries; i++) {
            switch (ThreadLocalRandom.current().nextInt(3)) {
                case 0 -> entries.add(randomConfigurationEntry());
                case 1 -> entries.add(new ClientRegistrationEntry(new Term(randomIntValue()), randomIntValue()));
                case 2 ->
                        entries.add(new StateMachineCommandEntry(new Term(randomIntValue()), randomIntValue(), randomIntValue(), randomIntValue(), randomByteArray()));
                default -> fail("Invalid value");
            }
        }
        return entries;
    }

    private ConfigurationEntry randomConfigurationEntry() {
        final int numClusterMembers = ThreadLocalRandom.current().nextInt(10);
        Set<Object> clusterMembers = HashSet.newHashSet(numClusterMembers);
        for (int i = 0; i < numClusterMembers; i++) {
            clusterMembers.add(randomIntValue());
        }
        return new ConfigurationEntry(new Term(randomIntValue()), clusterMembers);
    }

    @SuppressWarnings("unchecked")
    private static <T extends Streamable> void testRoundTrip(T message) {
        final ByteBufferIO streamingIO = ByteBufferIO.elastic(IntegerIDSerializer.INSTANCE);
        streamingIO.writeStreamable(message);
        assertThat((T) streamingIO.readStreamable()).usingRecursiveComparison()
                .isEqualTo(message);
    }
}