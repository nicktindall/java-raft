package au.id.tindall.distalg.raft.comms;

import au.id.tindall.distalg.raft.InboxFactory;
import au.id.tindall.distalg.raft.cluster.Configuration;
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
                dispatch(destinationId, new AppendEntriesRequest<>(currentTerm, localId, prevLogIndex, prevLogTerm, entriesToReplicate, commitIndex));
            }

            @Override
            public void sendAppendEntriesResponse(Term currentTerm, Long destinationId, boolean success, Optional<Integer> appendedIndex) {
                dispatch(destinationId, new AppendEntriesResponse<>(currentTerm, localId, success, appendedIndex));
            }

            @Override
            public void sendRequestVoteRequest(Configuration<Long> configuration, Term currentTerm, int lastLogIndex, Optional<Term> lastLogTerm, boolean earlyElection) {
                configuration.getServers().forEach(id -> dispatch(id, new RequestVoteRequest<>(currentTerm, localId, lastLogIndex, lastLogTerm, earlyElection)));
            }

            @Override
            public void sendRequestVoteResponse(Term currentTerm, Long destinationId, boolean granted) {
                dispatch(destinationId, new RequestVoteResponse<>(currentTerm, localId, granted));
            }

            @Override
            public void sendTimeoutNowRequest(Term currentTerm, Long destinationId) {
                dispatch(destinationId, new TimeoutNowMessage<>(currentTerm, localId, true));
            }

            @Override
            public void sendInstallSnapshotResponse(Term currentTerm, Long destinationId, boolean success, int lastIndex, int endOffset) {
                dispatch(destinationId, new InstallSnapshotResponse<>(currentTerm, localId, success, lastIndex, endOffset));
            }

            @Override
            public void sendInstallSnapshotRequest(Term currentTerm, Long destinationId, int lastIndex, Term lastTerm, ConfigurationEntry lastConfiguration, int snapshotOffset, int offset, byte[] data, boolean done) {
                dispatch(destinationId, new InstallSnapshotRequest<>(currentTerm, localId, lastIndex, lastTerm, lastConfiguration, snapshotOffset, offset, data, done));
            }
        };
    }

    public void logStats() {
        messageStats.logStats();
    }

    private void dispatch(Long destinationId, RpcMessage<Long> message) {
        long startTimeNanos = System.nanoTime();
        LOGGER.trace("Sending {} from Server {} to Server {}", message.getClass().getSimpleName(), message.getSource(), destinationId);
        servers.get(destinationId).send(message);
        messageStats.recordMessageSent(message);
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
