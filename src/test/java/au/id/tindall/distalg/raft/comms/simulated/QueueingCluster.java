package au.id.tindall.distalg.raft.comms.simulated;

import au.id.tindall.distalg.raft.cluster.Configuration;
import au.id.tindall.distalg.raft.comms.ClientConnection;
import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.comms.MessageProcessor;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.ConfigurationEntry;
import au.id.tindall.distalg.raft.log.entries.LogEntry;
import au.id.tindall.distalg.raft.rpc.server.AppendEntriesRequest;
import au.id.tindall.distalg.raft.rpc.server.AppendEntriesResponse;
import au.id.tindall.distalg.raft.rpc.server.RequestVoteRequest;
import au.id.tindall.distalg.raft.rpc.server.RequestVoteResponse;
import au.id.tindall.distalg.raft.rpc.server.RpcMessage;
import au.id.tindall.distalg.raft.rpc.server.TimeoutNowMessage;
import au.id.tindall.distalg.raft.rpc.snapshots.InstallSnapshotRequest;
import au.id.tindall.distalg.raft.rpc.snapshots.InstallSnapshotResponse;
import au.id.tindall.distalg.raft.util.Closeables;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

class QueueingCluster<I> implements Cluster<I>, Closeable {

    private static final Logger LOGGER = LogManager.getLogger();

    private final Consumer<RpcMessage<I>> noOp = msg -> {
    };
    private final I localId;
    private final Map<I, Queue<RpcMessage<I>>> outQueues = new ConcurrentHashMap<>();
    private final List<ClientConnection<I>> clientConnections;
    private final Queue<RpcMessage<I>> rpcMessages;
    private Consumer<RpcMessage<I>> messageSendListener = noOp;

    public QueueingCluster(I localId) {
        this.localId = localId;
        clientConnections = new CopyOnWriteArrayList<>();
        rpcMessages = new ConcurrentLinkedQueue<>();
    }

    private Queue<RpcMessage<I>> getPeerQueue(I id) {
        return outQueues.computeIfAbsent(id, k -> new ConcurrentLinkedQueue<>());
    }

    @Override
    public void onStart() {
        // Nothing to do
    }

    @Override
    public void onStop() {
        outQueues.clear();
    }

    @Override
    public void sendAppendEntriesRequest(Term currentTerm, I destinationId, int prevLogIndex, Optional<Term> prevLogTerm, List<LogEntry> entriesToReplicate, int commitIndex) {
        sendMessage(destinationId, new AppendEntriesRequest<>(currentTerm, localId, prevLogIndex, prevLogTerm, entriesToReplicate, commitIndex));
    }

    @Override
    public void sendAppendEntriesResponse(Term currentTerm, I destinationId, boolean success, Optional<Integer> appendedIndex) {
        sendMessage(destinationId, new AppendEntriesResponse<>(currentTerm, localId, success, appendedIndex));
    }

    @Override
    public void sendRequestVoteRequest(Configuration<I> configuration, Term currentTerm, int lastLogIndex, Optional<Term> lastLogTerm, boolean earlyElection) {
        configuration.getServers().forEach(id -> sendMessage(id, new RequestVoteRequest<>(currentTerm, configuration.getLocalId(), lastLogIndex, lastLogTerm, earlyElection)));
    }

    @Override
    public void sendRequestVoteResponse(Term currentTerm, I destinationId, boolean granted) {
        sendMessage(destinationId, new RequestVoteResponse<>(currentTerm, localId, granted));
    }

    @Override
    public void sendTimeoutNowRequest(Term currentTerm, I destinationId) {
        sendMessage(destinationId, new TimeoutNowMessage<>(currentTerm, localId, true));
    }

    @Override
    public void sendInstallSnapshotResponse(Term currentTerm, I destinationId, boolean success, int lastIndex, int endOffset) {
        sendMessage(destinationId, new InstallSnapshotResponse<>(currentTerm, localId, success, lastIndex, endOffset));
    }

    @Override
    public void sendInstallSnapshotRequest(Term currentTerm, I destinationId, int lastIndex, Term lastTerm, ConfigurationEntry lastConfiguration, int snapshotOffset, int offset, byte[] data, boolean done) {
        sendMessage(destinationId, new InstallSnapshotRequest<>(currentTerm, localId, lastIndex, lastTerm, lastConfiguration, snapshotOffset, offset, data, done));
    }

    public Map<I, Queue<RpcMessage<I>>> queues() {
        return outQueues;
    }

    public void setMessageSendListener(Consumer<RpcMessage<I>> messageSendListener) {
        this.messageSendListener = messageSendListener;
    }

    private void sendMessage(I destinationId, RpcMessage<I> rpcMessage) {
        LOGGER.trace("Sending message to {}:{}", destinationId, rpcMessage);
        messageSendListener.accept(rpcMessage);
        getPeerQueue(destinationId).offer(rpcMessage);
    }

    public void deliver(RpcMessage<I> rpcMessage) {
        rpcMessages.offer(rpcMessage);
    }

    @Override
    public boolean processNextMessage(MessageProcessor<I> messageProcessor) {
        final RpcMessage<I> rpcMessage = rpcMessages.poll();
        if (rpcMessage != null) {
            messageProcessor.handle(rpcMessage);
            return true;
        }
        for (final ClientConnection<I> conn : clientConnections) {
            if (conn.processAMessage(messageProcessor)) {
                return true;
            }
        }
        return false;
    }

    public ClientConnection<I> openConnection() {
        final ClientConnection<I> conn = new ClientConnection<>();
        clientConnections.add(conn);
        return conn;
    }

    @Override
    public void close() {
        Closeables.closeQuietly(clientConnections);
    }
}
