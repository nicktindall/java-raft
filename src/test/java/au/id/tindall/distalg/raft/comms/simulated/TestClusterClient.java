package au.id.tindall.distalg.raft.comms.simulated;

import au.id.tindall.distalg.raft.Server;
import au.id.tindall.distalg.raft.clusterclient.ClusterClient;
import au.id.tindall.distalg.raft.comms.ClientConnection;
import au.id.tindall.distalg.raft.comms.ConnectionClosedException;
import au.id.tindall.distalg.raft.exceptions.NotRunningException;
import au.id.tindall.distalg.raft.rpc.client.ClientRequestMessage;
import au.id.tindall.distalg.raft.rpc.client.ClientResponseMessage;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

class TestClusterClient<I> implements ClusterClient<I> {

    private final Map<I, Server<I>> servers;
    private final Map<I, ClientConnection<I>> serverConnections;

    public TestClusterClient(Map<I, Server<I>> servers) {
        this.servers = servers;
        this.serverConnections = new HashMap<>();
    }

    @Override
    public Set<I> getClusterNodeIds() {
        return servers.keySet();
    }

    @Override
    public <R extends ClientResponseMessage<I>> CompletableFuture<R> send(I destination, ClientRequestMessage<I, R> clientRequestMessage) {
        final ClientConnection<I> connection = getConnection(servers.get(destination));
        try {
            return connection.send(clientRequestMessage);
        } catch (NotRunningException ex) {
            throw new ConnectionClosedException();
        }
    }

    private ClientConnection<I> getConnection(Server<I> leader) {
        return serverConnections.compute(leader.getId(), (key, existing) -> {
            if (existing == null || existing.isClosed()) {
                return ((QueueingCluster<I>) leader.getCluster()).openConnection();
            }
            return existing;
        });
    }
}
