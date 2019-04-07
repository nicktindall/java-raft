package au.id.tindall.distalg.raft.comms;

import static java.util.function.Function.identity;
import static org.apache.logging.log4j.LogManager.getLogger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import au.id.tindall.distalg.raft.Server;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.LogEntry;
import au.id.tindall.distalg.raft.rpc.server.AppendEntriesRequest;
import au.id.tindall.distalg.raft.rpc.server.AppendEntriesResponse;
import au.id.tindall.distalg.raft.rpc.server.BroadcastMessage;
import au.id.tindall.distalg.raft.rpc.server.RequestVoteRequest;
import au.id.tindall.distalg.raft.rpc.server.RequestVoteResponse;
import au.id.tindall.distalg.raft.rpc.server.RpcMessage;
import au.id.tindall.distalg.raft.rpc.server.UnicastMessage;
import org.apache.logging.log4j.Logger;

public class TestCluster {

    private static final Logger LOGGER = getLogger();

    private Map<Long, Server<Long>> servers;
    private List<RpcMessage<Long>> messageQueue;

    public TestCluster() {
        messageQueue = new ArrayList<>();
    }

    @SafeVarargs
    public final void setServers(Server<Long>... servers) {
        this.servers = Arrays.stream(servers).collect(Collectors.toMap(Server::getId, identity()));
    }

    public boolean isQuorum(Set<Long> receivedVotes) {
        return receivedVotes.size() > (servers.size() / 2f);
    }

    public Set<Long> getMemberIds() {
        return servers.keySet();
    }

    private void send(RpcMessage<Long> message) {
        messageQueue.add(message);
    }

    public Cluster<Long> forServer(long localId) {
        return new Cluster<>() {

            @Override
            public boolean isQuorum(Set<Long> receivedVotes) {
                return TestCluster.this.isQuorum(receivedVotes);
            }

            @Override
            public Set<Long> getMemberIds() {
                return TestCluster.this.getMemberIds();
            }

            @Override
            public void sendAppendEntriesRequest(Term currentTerm, Long destinationId, int prevLogIndex, Optional<Term> prevLogTerm, List<LogEntry> entriesToReplicate, int commitIndex) {
                send(new AppendEntriesRequest<>(currentTerm, localId, destinationId, prevLogIndex, prevLogTerm, entriesToReplicate, commitIndex));
            }

            @Override
            public void sendAppendEntriesResponse(Term currentTerm, Long destinationId, boolean success, Optional<Integer> appendedIndex) {
                send(new AppendEntriesResponse<>(currentTerm, localId, destinationId, success, appendedIndex));
            }

            @Override
            public void sendRequestVoteRequest(Term currentTerm, int lastLogIndex, Optional<Term> lastLogTerm) {
                send(new RequestVoteRequest<>(currentTerm, localId, lastLogIndex, lastLogTerm));
            }

            @Override
            public void sendRequestVoteResponse(Term currentTerm, Long destinationId, boolean granted) {
                send(new RequestVoteResponse<>(currentTerm, localId, destinationId, granted));
            }
        };
    }

    public void flush() {
        List<RpcMessage<Long>> oldMessages = messageQueue;
        messageQueue = new ArrayList<>();
        oldMessages.forEach(this::dispatch);
    }

    public void fullyFlush() {
        while (!messageQueue.isEmpty()) {
            flush();
        }
    }

    private void dispatch(RpcMessage<Long> message) {
        if (message instanceof BroadcastMessage) {
            LOGGER.info("Broadcasting {} from Server {}", message.getClass().getSimpleName(), message.getSource());
            servers.values().forEach(server -> server.handle(message));
        } else if (message instanceof UnicastMessage) {
            LOGGER.info("Sending {} from Server {} to Server {}", message.getClass().getSimpleName(), message.getSource(), ((UnicastMessage<Long>) message).getDestination());
            servers.get(((UnicastMessage<Long>) message).getDestination()).handle(message);
        } else {
            throw new IllegalStateException("Unknown message type: " + message.getClass().getName());
        }
    }
}
