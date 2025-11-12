package au.id.tindall.distalg.raft.comms.netty.simplemesh;

import au.id.tindall.distalg.raft.serialisation.MessageIdentifier;
import au.id.tindall.distalg.raft.serialisation.Streamable;
import au.id.tindall.distalg.raft.serialisation.StreamingInput;
import au.id.tindall.distalg.raft.serialisation.StreamingOutput;
import au.id.tindall.distalg.raft.util.ThreadUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class NodeTest {

    private static final Logger LOGGER = LogManager.getLogger();

    @Test
    void nodesCanSendToEachOther() {
        String node1address = "localhost:12301";
        String node2address = "localhost:12302";
        String node3address = "localhost:12303";
        String[] allAddresses = new String[]{node1address, node2address, node3address};

        try (Node node1 = new Node(node1address, allAddresses, 1);
             Node node2 = new Node(node2address, allAddresses, 2);
             Node node3 = new Node(node3address, allAddresses, 3)) {
            node1.start();
            node2.start();
            node3.start();

            await().atMost(10, TimeUnit.SECONDS)
                    .until(() -> nodesAreFullyConnected(node1, node2, node3));

            Node[] allNodes = new Node[]{node1, node2, node3};
            assertAllNodesCanCommunicate(allNodes);
        }
    }

    @Test
    void nodeWillConnectWhenNodeStarts() {
        String node1address = "localhost:12301";
        String node2address = "localhost:12302";
        String[] allAddresses = new String[]{node1address, node2address};

        try (Node node1 = new Node(node1address, allAddresses, 1);
             Node node2 = new Node(node2address, allAddresses, 2)) {
            node1.start();

            ThreadUtil.pauseMillis(2_000);

            node2.start();

            await().atMost(10, TimeUnit.SECONDS)
                    .until(() -> nodesAreFullyConnected(node1, node2));
        }
    }

    @Test
    void newNodeWillFindExistingNodesThroughGossip() {
        String node1address = "localhost:12301";
        String node2address = "localhost:12302";
        String node3address = "localhost:12303";
        String newNodeAddress = "localhost:12304";
        String[] initialAddresses = new String[]{node1address, node2address, node3address};

        try (Node node1 = new Node(node1address, initialAddresses, 1);
             Node node2 = new Node(node2address, initialAddresses, 2);
             Node node3 = new Node(node3address, initialAddresses, 3);
             Node newNode = new Node(newNodeAddress, new String[]{node2address}, 4)) {
            node1.start();
            node2.start();
            node3.start();
            await().atMost(10, TimeUnit.SECONDS)
                    .until(() -> nodesAreFullyConnected(node1, node2, node3));

            LOGGER.info("Starting newNode");
            newNode.start();
            await().atMost(10, TimeUnit.SECONDS)
                    .until(() -> nodesAreFullyConnected(node1, node2, node3, newNode));
        }
    }

    @Test
    void newerNodesTakePrecedenceOverOldNodes() {
        String node1address = "localhost:12301";
        String node2address = "localhost:12302";
        String existingNodeAddress = "localhost:12303";
        String replacementNodeAddress = "localhost:12304";
        String[] initialAddresses = new String[]{node1address, node2address};

        try (Node node1 = new Node(node1address, initialAddresses, 1);
             Node node2 = new Node(node2address, initialAddresses, 2)) {
            node1.start();
            node2.start();
            final int replacedId = 99;
            try (Node existingNode = new Node(existingNodeAddress, initialAddresses, replacedId)) {
                LOGGER.info("Starting existingNode");
                existingNode.start();
                await().atMost(10, TimeUnit.SECONDS)
                        .until(() -> nodesAreFullyConnected(node1, node2, existingNode));
                assertAllNodesCanCommunicate(node1, node2, existingNode);

                try (Node replacemenetNode = new Node(replacementNodeAddress, initialAddresses, replacedId)) {
                    LOGGER.info("Starting newNode");
                    replacemenetNode.start();

                    ThreadUtil.pauseMillis(2_000);

                    LOGGER.info("n1={}, n2={}, existing={}, replacement={}", node1, node2, existingNode, replacemenetNode);

                    await().atMost(10, TimeUnit.SECONDS)
                            .until(() -> {
                                Set<Integer> connectedNodeIds = existingNode.getConnectedNodeIds();
                                LOGGER.info("Connected to {}", connectedNodeIds);
                                return connectedNodeIds.isEmpty();
                            });

                    await().atMost(10, TimeUnit.SECONDS).
                            until(() -> nodesAreFullyConnected(node1, node2, replacemenetNode));
                    assertAllNodesCanCommunicate(node1, node2, replacemenetNode);
                }
            }
        }
    }

    @Test
    void canStopAndStartANode() {
        String node1address = "localhost:12301";
        String node2address = "localhost:12302";
        String node3address = "localhost:12303";
        String[] initialAddresses = new String[]{node1address, node2address, node3address};

        try (Node node1 = new Node(node1address, initialAddresses, 1);
             Node node2 = new Node(node2address, initialAddresses, 2);
             Node node3 = new Node(node3address, initialAddresses, 3)) {
            node1.start();
            node2.start();
            node3.start();
            await().atMost(10, TimeUnit.SECONDS)
                    .until(() -> nodesAreFullyConnected(node1, node2, node3));

            for (Node node : List.of(node1, node2, node3)) {
                node.stop();
                await().atMost(10, TimeUnit.SECONDS)
                        .until(() -> node.getConnectedNodeIds().isEmpty());

                node.start();
                await().atMost(10, TimeUnit.SECONDS)
                        .until(() -> nodesAreFullyConnected(node1, node2, node3));
            }
        }
    }

    public static class PayloadMessage implements Streamable {
        private static final MessageIdentifier MESSAGE_IDENTIFIER = MessageIdentifier.registerMessageIdentifier("SimpleMesh.Test.Message", PayloadMessage.class);
        private final UUID id;

        public PayloadMessage(UUID id) {
            this.id = id;
        }

        public PayloadMessage(StreamingInput streamingInput) {
            long lsb = streamingInput.readLong();
            long msb = streamingInput.readLong();
            this.id = new UUID(msb, lsb);
        }

        public UUID getId() {
            return id;
        }

        @Override
        public MessageIdentifier getMessageIdentifier() {
            return MESSAGE_IDENTIFIER;
        }

        @Override
        public void writeTo(StreamingOutput streamingOutput) {
            streamingOutput.writeLong(id.getLeastSignificantBits());
            streamingOutput.writeLong(id.getMostSignificantBits());
        }
    }

    private static void assertAllNodesCanCommunicate(Node... allNodes) {
        for (Node sourceNode : allNodes) {
            for (Node destinationNode : allNodes) {
                if (sourceNode == destinationNode) {
                    continue;
                }
                final UUID id = UUID.randomUUID();
                sourceNode.sendMessage(destinationNode.getLocalId(), new PayloadMessage(id));
                Optional<Message> received;
                while ((received = destinationNode.pollFrom(sourceNode.getLocalId())).isEmpty()) {
                    // repeat
                }
                assertThat(((PayloadMessage) received.get().message()).getId()).isEqualTo(id);
            }
        }
    }

    private static boolean nodesAreFullyConnected(Node... allNodes) {
        for (Node node : allNodes) {
            for (Node otherNode : allNodes) {
                if (node.getLocalId() != otherNode.getLocalId()
                        && !node.isConnectedTo(otherNode.getLocalId())) {
                    return false;
                }
            }
        }
        return true;
    }
}