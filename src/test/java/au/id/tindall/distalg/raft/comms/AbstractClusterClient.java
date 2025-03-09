package au.id.tindall.distalg.raft.comms;

import au.id.tindall.distalg.raft.Server;
import au.id.tindall.distalg.raft.exceptions.NotRunningException;
import au.id.tindall.distalg.raft.rpc.client.ClientRequestMessage;
import au.id.tindall.distalg.raft.rpc.client.ClientResponseMessage;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static au.id.tindall.distalg.raft.serverstates.ServerStateType.LEADER;

public abstract class AbstractClusterClient {

    private static final int MAX_RETRIES = 20;

    private final Map<Long, Server<Long>> servers;
    private final Map<Long, ClientConnection<Long>> serverConnections;

    public AbstractClusterClient(Map<Long, Server<Long>> servers) {
        this.servers = servers;
        this.serverConnections = new HashMap<>();
    }

    protected <R extends ClientResponseMessage> CompletableFuture<R> sendClientRequest(ClientRequestMessage<R> request) {
        int retries = 0;
        while (retries < MAX_RETRIES) {
            Server<Long> leader = findLeader();
            ClientConnection<Long> connection = getConnection(leader);
            try {
                return connection.send(request);
            } catch (NotRunningException ex) {
                // Do nothing we'll retry
            }
            retries++;
        }
        throw new IllegalStateException("Maximum retries exceeded, failing sending...");
    }

    private ClientConnection<Long> getConnection(Server<Long> leader) {
        ClientConnection<Long> connection = serverConnections.get(leader.getId());
        if (connection == null || connection.isClosed()) {
            connection = ((QueueingInbox) leader.getInbox()).openConnection();
            serverConnections.put(leader.getId(), connection);
        }
        return connection;
    }

    private Server<Long> findLeader() {
        Optional<Server<Long>> leader = Optional.empty();
        while (leader.isEmpty()) {
            leader = servers.values().stream()
                    .filter(this::serverIsLeader)
                    .findAny();
        }
        return leader.get();
    }

    private boolean serverIsLeader(Server<Long> server) {
        return server.getState()
                .filter(state -> state == LEADER)
                .isPresent();
    }
}
