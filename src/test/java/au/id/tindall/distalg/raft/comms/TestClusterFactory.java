package au.id.tindall.distalg.raft.comms;

import au.id.tindall.distalg.raft.Server;
import au.id.tindall.distalg.raft.exceptions.NotRunningException;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.LogEntry;
import au.id.tindall.distalg.raft.rpc.server.AppendEntriesRequest;
import au.id.tindall.distalg.raft.rpc.server.AppendEntriesResponse;
import au.id.tindall.distalg.raft.rpc.server.BroadcastMessage;
import au.id.tindall.distalg.raft.rpc.server.RequestVoteRequest;
import au.id.tindall.distalg.raft.rpc.server.RequestVoteResponse;
import au.id.tindall.distalg.raft.rpc.server.RpcMessage;
import au.id.tindall.distalg.raft.rpc.server.UnicastMessage;
import org.apache.logging.log4j.CloseableThreadContext;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.logging.log4j.LogManager.getLogger;

public class TestClusterFactory implements ClusterFactory<Long> {

    private static final Logger LOGGER = getLogger();

    private final SendingStrategy sendingStrategy;
    private final Map<Long, Server<Long>> servers;
    private final MessageStats messageStats;

    public TestClusterFactory(SendingStrategy sendingStrategy, Map<Long, Server<Long>> servers) {
        this.sendingStrategy = sendingStrategy;
        this.servers = servers;
        this.sendingStrategy.setDispatchFunction(this::dispatch);
        this.messageStats = new MessageStats();
    }

    public boolean isQuorum(Set<Long> receivedVotes) {
        return receivedVotes.size() > (servers.size() / 2f);
    }

    public Set<Long> getMemberIds() {
        return servers.keySet();
    }

    @Override
    public Cluster<Long> createForNode(Long localId) {
        return new Cluster<>() {

            @Override
            public boolean isQuorum(Set<Long> receivedVotes) {
                return TestClusterFactory.this.isQuorum(receivedVotes);
            }

            @Override
            public Set<Long> getOtherMemberIds() {
                return TestClusterFactory.this.getMemberIds().stream()
                        .filter(id -> !id.equals(localId))
                        .collect(Collectors.toSet());
            }

            @Override
            public void sendAppendEntriesRequest(Term currentTerm, Long destinationId, int prevLogIndex, Optional<Term> prevLogTerm, List<LogEntry> entriesToReplicate, int commitIndex) {
                sendingStrategy.send(new AppendEntriesRequest<>(currentTerm, localId, destinationId, prevLogIndex, prevLogTerm, entriesToReplicate, commitIndex));
            }

            @Override
            public void sendAppendEntriesResponse(Term currentTerm, Long destinationId, boolean success, Optional<Integer> appendedIndex) {
                sendingStrategy.send(new AppendEntriesResponse<>(currentTerm, localId, destinationId, success, appendedIndex));
            }

            @Override
            public void sendRequestVoteRequest(Term currentTerm, int lastLogIndex, Optional<Term> lastLogTerm) {
                sendingStrategy.send(new RequestVoteRequest<>(currentTerm, localId, lastLogIndex, lastLogTerm));
            }

            @Override
            public void sendRequestVoteResponse(Term currentTerm, Long destinationId, boolean granted) {
                sendingStrategy.send(new RequestVoteResponse<>(currentTerm, localId, destinationId, granted));
            }
        };
    }

    public void logStats() {
        messageStats.logStats();
    }

    private void dispatch(RpcMessage<Long> message) {
        if (message instanceof BroadcastMessage) {
            LOGGER.info("Broadcasting {} from Server {}", message.getClass().getSimpleName(), message.getSource());
            servers.values().forEach(server -> {
                deliverMessageIfServerIsRunning(message, server);
                messageStats.recordMessageSent(message);
            });
        } else if (message instanceof UnicastMessage) {
            LOGGER.info("Sending {} from Server {} to Server {}", message.getClass().getSimpleName(), message.getSource(), ((UnicastMessage<Long>) message).getDestination());
            deliverMessageIfServerIsRunning(message, servers.get(((UnicastMessage<Long>) message).getDestination()));
            messageStats.recordMessageSent(message);
        } else {
            throw new IllegalStateException("Unknown message type: " + message.getClass().getName());
        }
    }

    private void deliverMessageIfServerIsRunning(RpcMessage<Long> message, Server<Long> server) {
        try (CloseableThreadContext.Instance ctc = CloseableThreadContext.put("serverId", server.getId().toString())) {
            if (server.getState().isPresent()) {
                try {
                    server.handle(message);
                } catch (NotRunningException ex) {
                    // Ignore, this can happen sometimes
                }
            }
        }
    }
}
