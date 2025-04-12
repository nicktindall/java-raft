package au.id.tindall.distalg.raft.clusterclient;

import au.id.tindall.distalg.raft.rpc.clustermembership.AbdicateLeadershipRequest;
import au.id.tindall.distalg.raft.rpc.clustermembership.AddServerRequest;
import au.id.tindall.distalg.raft.rpc.clustermembership.AddServerResponse;
import au.id.tindall.distalg.raft.rpc.clustermembership.RemoveServerRequest;
import au.id.tindall.distalg.raft.rpc.clustermembership.RemoveServerResponse;

import java.util.concurrent.ExecutionException;

public class ClusterAdminClient<I> extends AbstractClusterClient<I> {

    private final long requestTimeoutMs;

    public ClusterAdminClient(ClusterClient<I> clusterClient, long requestTimeoutMs) {
        super(clusterClient);
        this.requestTimeoutMs = requestTimeoutMs;
    }

    public AddServerResponse<I> addNewServer(I newServerId) throws ExecutionException, InterruptedException {
        return sendClientRequest(new AddServerRequest<>(newServerId), requestTimeoutMs).get();
    }

    public RemoveServerResponse<I> removeServer(I newServerId) throws ExecutionException, InterruptedException {
        return sendClientRequest(new RemoveServerRequest<>(newServerId), requestTimeoutMs).get();
    }

    public void deposeLeader() throws ExecutionException, InterruptedException {
        sendClientRequest(new AbdicateLeadershipRequest<>(), requestTimeoutMs).get();
    }
}
