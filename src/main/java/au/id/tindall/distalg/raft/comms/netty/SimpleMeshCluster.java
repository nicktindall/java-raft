package au.id.tindall.distalg.raft.comms.netty;

import au.id.tindall.distalg.raft.cluster.Configuration;
import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.comms.MessageProcessor;
import au.id.tindall.distalg.raft.comms.netty.simplemesh.Message;
import au.id.tindall.distalg.raft.comms.netty.simplemesh.Node;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.ConfigurationEntry;
import au.id.tindall.distalg.raft.log.entries.LogEntry;
import au.id.tindall.distalg.raft.rpc.client.ClientRequestMessage;
import au.id.tindall.distalg.raft.rpc.client.ClientResponseMessage;
import au.id.tindall.distalg.raft.rpc.server.AppendEntriesRequest;
import au.id.tindall.distalg.raft.rpc.server.AppendEntriesResponse;
import au.id.tindall.distalg.raft.rpc.server.RequestVoteRequest;
import au.id.tindall.distalg.raft.rpc.server.RequestVoteResponse;
import au.id.tindall.distalg.raft.rpc.server.RpcMessage;
import au.id.tindall.distalg.raft.rpc.server.TimeoutNowMessage;
import au.id.tindall.distalg.raft.rpc.snapshots.InstallSnapshotRequest;
import au.id.tindall.distalg.raft.rpc.snapshots.InstallSnapshotResponse;
import au.id.tindall.distalg.raft.serialisation.Streamable;
import au.id.tindall.distalg.raft.util.Closeables;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

public class SimpleMeshCluster implements Cluster<Integer>, Closeable {

    private static final Logger LOGGER = LogManager.getLogger();

    private final int localId;
    private final Node localNode;

    public SimpleMeshCluster(int localId, Node localNode) {
        this.localId = localId;
        this.localNode = localNode;
    }

    @Override
    public void onStart() {
        this.localNode.start();
    }

    @Override
    public void onStop() {
        this.localNode.stop();
    }

    @Override
    public void sendAppendEntriesRequest(Term currentTerm, Integer destinationId, int prevLogIndex, Optional<Term> prevLogTerm, List<LogEntry> entriesToReplicate, int commitIndex) {
        final AppendEntriesRequest<Integer> appendEntriesRequest =
                new AppendEntriesRequest<>(currentTerm, localId, prevLogIndex, prevLogTerm, entriesToReplicate, commitIndex);
        sendMessage(destinationId, appendEntriesRequest);
    }


    @Override
    public void sendAppendEntriesResponse(Term currentTerm, Integer destinationId, boolean success, Optional<Integer> appendedIndex) {
        sendMessage(destinationId, new AppendEntriesResponse<>(currentTerm, localId, success, appendedIndex));
    }

    @Override
    public void sendRequestVoteRequest(Configuration<Integer> configuration, Term currentTerm, int lastLogIndex, Optional<Term> lastLogTerm, boolean earlyElection) {
        configuration.getServers()
                .forEach(serverId -> sendMessage(serverId, new RequestVoteRequest<>(currentTerm, localId, lastLogIndex, lastLogTerm, earlyElection)));
    }

    @Override
    public void sendRequestVoteResponse(Term currentTerm, Integer destinationId, boolean granted) {
        sendMessage(destinationId, new RequestVoteResponse<>(currentTerm, localId, granted));
    }

    @Override
    public void sendTimeoutNowRequest(Term currentTerm, Integer destinationId) {
        sendMessage(destinationId, new TimeoutNowMessage<>(currentTerm, localId, true));
    }

    @Override
    public void sendInstallSnapshotResponse(Term currentTerm, Integer destinationId, boolean success, int lastIndex, int endOffset) {
        sendMessage(destinationId, new InstallSnapshotResponse<>(currentTerm, localId, success, lastIndex, endOffset));
    }

    @Override
    public void sendInstallSnapshotRequest(Term currentTerm, Integer destinationId, int lastIndex, Term lastTerm, ConfigurationEntry lastConfiguration, int snapshotOffset, int offset, byte[] data, boolean done) {
        sendMessage(destinationId, new InstallSnapshotRequest<>(currentTerm, localId, lastIndex, lastTerm, lastConfiguration, snapshotOffset, offset, data, done));
    }

    @Override
    public boolean processNextMessage(MessageProcessor<Integer> messageProcessor) {
        final Optional<Message> receivedMessage = localNode.poll();
        receivedMessage.ifPresent(message -> {
            Streamable raftMessage = message.message();
            switch (raftMessage) {
                case RpcMessage<?> rpcMessage -> messageProcessor.handle((RpcMessage<Integer>) rpcMessage);
                case MessageEnvelope clientRequestMessage ->
                        handleClientMessage(message.source(), clientRequestMessage, messageProcessor);
                default -> LOGGER.warn("Received message of unexpected type, ignoring: {}", raftMessage);
            }
        });
        return receivedMessage.isPresent();
    }

    private <R extends ClientResponseMessage<Integer>> void handleClientMessage(int source, MessageEnvelope messageEnvelope, MessageProcessor<Integer> messageProcessor) {
        switch (messageEnvelope.message()) {
            case ClientRequestMessage clientRequestMessage ->
                    messageProcessor.handle((ClientRequestMessage<Integer, R>) clientRequestMessage)
                            .thenAccept(response -> sendMessage(source, new MessageEnvelope(messageEnvelope.correlationId(), response)));
            default -> LOGGER.warn("Received message of unexpected type, ignoring: {}", messageEnvelope.message());
        }
    }

    private void sendMessage(int destinationId, Streamable message) {
        localNode.sendMessage(destinationId, message);
    }

    @Override
    public void close() throws IOException {
        Closeables.closeQuietly(localNode);
    }
}
