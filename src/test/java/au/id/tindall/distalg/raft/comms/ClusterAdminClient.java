package au.id.tindall.distalg.raft.comms;

import au.id.tindall.distalg.raft.Server;
import au.id.tindall.distalg.raft.rpc.clustermembership.AddServerRequest;
import au.id.tindall.distalg.raft.rpc.clustermembership.AddServerResponse;
import au.id.tindall.distalg.raft.rpc.clustermembership.RemoveServerRequest;
import au.id.tindall.distalg.raft.rpc.clustermembership.RemoveServerResponse;
import au.id.tindall.distalg.raft.rpc.clustermembership.TransferLeadershipRequest;

import java.util.Map;
import java.util.concurrent.ExecutionException;

public class ClusterAdminClient extends AbstractClusterClient {

    public ClusterAdminClient(Map<Long, Server<Long>> servers) {
        super(servers);
    }

    public AddServerResponse addNewServer(long newServerId) throws ExecutionException, InterruptedException {
        return ((AddServerResponse) sendServerAdminRequest(id -> new AddServerRequest<>(newServerId)).get());
    }

    public RemoveServerResponse removeServer(long newServerId) throws ExecutionException, InterruptedException {
        return ((RemoveServerResponse) sendServerAdminRequest(id -> new RemoveServerRequest<>(newServerId)).get());
    }

    public void transferLeadership() throws ExecutionException, InterruptedException {
        sendServerAdminRequest(id -> new TransferLeadershipRequest()).get();
    }
}
