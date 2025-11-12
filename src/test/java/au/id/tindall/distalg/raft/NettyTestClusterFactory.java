package au.id.tindall.distalg.raft;

import au.id.tindall.distalg.raft.clusterclient.ClusterClient;
import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.comms.TestInfrastructureFactory;
import au.id.tindall.distalg.raft.comms.netty.SimpleMeshCluster;
import au.id.tindall.distalg.raft.comms.netty.SimpleMeshClusterClient;
import au.id.tindall.distalg.raft.comms.netty.simplemesh.Node;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class NettyTestClusterFactory implements TestInfrastructureFactory<Integer> {

    private final AtomicInteger clientIdCounter = new AtomicInteger(Integer.MAX_VALUE - 100_000);
    private final Set<String> activeNodeAddresses = Collections.newSetFromMap(new ConcurrentHashMap<>());

    @Override
    public Cluster<Integer> createCluster(Integer serverId) {
        final NodeResources nodeResources = createNewNodeResources(serverId);
        activeNodeAddresses.add(nodeResources.localAddress());
        return new SimpleMeshCluster(serverId, nodeResources.node()) {
            @Override
            public void close() throws IOException {
                super.close();
                activeNodeAddresses.remove(nodeResources.localAddress());
            }
        };
    }

    @Override
    public void close() {
        activeNodeAddresses.clear();
    }

    private NodeResources createNewNodeResources(Integer nodeId) {
        String address = getUnusedAddress();
        final Node node = new Node(address, getInitialPeerAddresses(), nodeId);
        return new NodeResources(address, node);
    }

    private String getUnusedAddress() {
        try (var ignored = new ServerSocket(0)) {
            ignored.setReuseAddress(true);
            return "127.0.0.1:" + ignored.getLocalPort();
        } catch (IOException e) {
            throw new IllegalStateException("Can't allocate an address", e);
        }
    }

    public String[] getInitialPeerAddresses() {
        return activeNodeAddresses.stream()
                .limit(3)
                .toArray(String[]::new);
    }

    @Override
    public ClusterClient<Integer> createClusterClient() {
        final NodeResources clientResources = createNewNodeResources(clientIdCounter.incrementAndGet());
        clientResources.node().start();
        activeNodeAddresses.add(clientResources.localAddress());
        return new SimpleMeshClusterClient(clientResources.node()) {
            @Override
            public void close() {
                super.close();
                activeNodeAddresses.remove(clientResources.localAddress());
            }
        };
    }

    private record NodeResources(String localAddress, Node node) {
    }
}
