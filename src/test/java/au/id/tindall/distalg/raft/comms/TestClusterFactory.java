package au.id.tindall.distalg.raft.comms;

import au.id.tindall.distalg.raft.InboxFactory;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.ConfigurationEntry;
import au.id.tindall.distalg.raft.log.entries.LogEntry;
import au.id.tindall.distalg.raft.rpc.server.AppendEntriesRequest;
import au.id.tindall.distalg.raft.rpc.server.AppendEntriesResponse;
import au.id.tindall.distalg.raft.rpc.server.BroadcastMessage;
import au.id.tindall.distalg.raft.rpc.server.RequestVoteRequest;
import au.id.tindall.distalg.raft.rpc.server.RequestVoteResponse;
import au.id.tindall.distalg.raft.rpc.server.RpcMessage;
import au.id.tindall.distalg.raft.rpc.server.TimeoutNowMessage;
import au.id.tindall.distalg.raft.rpc.server.UnicastMessage;
import au.id.tindall.distalg.raft.rpc.snapshots.InstallSnapshotRequest;
import au.id.tindall.distalg.raft.rpc.snapshots.InstallSnapshotResponse;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.logging.log4j.LogManager.getLogger;

public class TestClusterFactory implements ClusterFactory<Long>, InboxFactory<Long> {

    private static final long SEND_WARN_THRESHOLD_US = 10_000;
    private static final Logger LOGGER = getLogger();

    private final SendingStrategy sendingStrategy;
    private final Map<Long, QueueingInbox> servers;
    private final MessageStats messageStats;

    public TestClusterFactory(SendingStrategy sendingStrategy) {
        this.sendingStrategy = sendingStrategy;
        this.servers = new ConcurrentHashMap<>();
        this.messageStats = new MessageStats();
    }

    @Override
    public Cluster<Long> createCluster(Long localId) {
        return new Cluster<>() {

            @Override
            public void onStart() {
                servers.get(localId).reset();
            }

            @Override
            public void onStop() {
            }

            @Override
            public void sendAppendEntriesRequest(Term currentTerm, Long destinationId, int prevLogIndex, Optional<Term> prevLogTerm, List<LogEntry> entriesToReplicate, int commitIndex) {
                dispatch(new AppendEntriesRequest<>(currentTerm, localId, destinationId, prevLogIndex, prevLogTerm, entriesToReplicate, commitIndex));
            }

            @Override
            public void sendAppendEntriesResponse(Term currentTerm, Long destinationId, boolean success, Optional<Integer> appendedIndex) {
                dispatch(new AppendEntriesResponse<>(currentTerm, localId, destinationId, success, appendedIndex));
            }

            @Override
            public void sendRequestVoteRequest(Term currentTerm, int lastLogIndex, Optional<Term> lastLogTerm) {
                dispatch(new RequestVoteRequest<>(currentTerm, localId, lastLogIndex, lastLogTerm));
            }

            @Override
            public void sendRequestVoteResponse(Term currentTerm, Long destinationId, boolean granted) {
                dispatch(new RequestVoteResponse<>(currentTerm, localId, destinationId, granted));
            }

            @Override
            public void sendTimeoutNowRequest(Term currentTerm, Long destinationId) {
                dispatch(new TimeoutNowMessage<>(currentTerm, localId, destinationId));
            }

            @Override
            public void sendInstallSnapshotResponse(Term currentTerm, Long destinationId, boolean success, int lastIndex, int endOffset) {
                dispatch(new InstallSnapshotResponse<>(currentTerm, localId, destinationId, success, lastIndex, endOffset));
            }

            @Override
            public void sendInstallSnapshotRequest(Term currentTerm, Long destinationId, int lastIndex, Term lastTerm, ConfigurationEntry lastConfiguration, int snapshotOffset, int offset, byte[] data, boolean done) {
                dispatch(new InstallSnapshotRequest<>(currentTerm, localId, destinationId, lastIndex, lastTerm, lastConfiguration, snapshotOffset, offset, data, done));
            }
        };
    }

    public void logStats() {
        messageStats.logStats();
    }

    private void dispatch(RpcMessage<Long> message) {
        long startTimeNanos = System.nanoTime();
        if (message instanceof BroadcastMessage) {
            LOGGER.trace("Broadcasting {} from Server {}", message.getClass().getSimpleName(), message.getSource());
            servers.values().forEach(server -> {
                server.send(message);
                messageStats.recordMessageSent(message);
            });
        } else if (message instanceof UnicastMessage) {
            final Long destinationId = ((UnicastMessage<Long>) message).getDestination();
            LOGGER.trace("Sending {} from Server {} to Server {}", message.getClass().getSimpleName(), message.getSource(), destinationId);
            servers.get(destinationId).send(message);
            messageStats.recordMessageSent(message);
        } else {
            throw new IllegalStateException("Unknown message type: " + message.getClass().getName());
        }
        long sendDurationMicros = (System.nanoTime() - startTimeNanos) / 1_000;
        if (sendDurationMicros > SEND_WARN_THRESHOLD_US) {
            LOGGER.warn("Sending to cluster took {}us", sendDurationMicros);
        }
    }

    @Override
    public Inbox<Long> createInbox(Long node) {
        return servers.computeIfAbsent(node, n -> new QueueingInbox(n, sendingStrategy));
    }
}
