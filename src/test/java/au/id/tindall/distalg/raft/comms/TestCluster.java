package au.id.tindall.distalg.raft.comms;

import static java.util.function.Function.identity;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import au.id.tindall.distalg.raft.Server;
import au.id.tindall.distalg.raft.rpc.server.BroadcastMessage;
import au.id.tindall.distalg.raft.rpc.server.RpcMessage;
import au.id.tindall.distalg.raft.rpc.server.UnicastMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TestCluster implements Cluster<Long> {

    private static final Logger LOGGER = LogManager.getLogger();

    private Map<Long, Server<Long>> servers;
    private List<RpcMessage<Long>> messageQueue;

    public TestCluster() {
        messageQueue = new ArrayList<>();
    }

    @SafeVarargs
    public final void setServers(Server<Long>... servers) {
        this.servers = Arrays.stream(servers).collect(Collectors.toMap(Server::getId, identity()));
    }

    @Override
    public void send(RpcMessage<Long> message) {
        messageQueue.add(message);
    }

    @Override
    public boolean isQuorum(Set<Long> receivedVotes) {
        return receivedVotes.size() > (servers.size() / 2f);
    }

    @Override
    public Set<Long> getMemberIds() {
        return servers.keySet();
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
