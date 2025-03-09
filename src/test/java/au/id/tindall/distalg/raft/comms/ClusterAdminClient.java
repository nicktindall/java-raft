package au.id.tindall.distalg.raft.comms;

import au.id.tindall.distalg.raft.Server;
import au.id.tindall.distalg.raft.rpc.clustermembership.AbdicateLeadershipRequest;
import au.id.tindall.distalg.raft.rpc.clustermembership.AbdicateLeadershipResponse;
import au.id.tindall.distalg.raft.rpc.clustermembership.AddServerRequest;
import au.id.tindall.distalg.raft.rpc.clustermembership.AddServerResponse;
import au.id.tindall.distalg.raft.rpc.clustermembership.RemoveServerRequest;
import au.id.tindall.distalg.raft.rpc.clustermembership.RemoveServerResponse;

import java.util.Map;
import java.util.concurrent.ExecutionException;

public class ClusterAdminClient extends AbstractClusterClient {

    public ClusterAdminClient(Map<Long, Server<Long>> servers) {
        super(servers);
    }

    public AddServerResponse addNewServer(long newServerId) throws ExecutionException, InterruptedException {
        return sendClientRequest(new AddServerRequest<>(newServerId)).get();
    }

    public RemoveServerResponse removeServer(long newServerId) throws ExecutionException, InterruptedException {
        return sendClientRequest(new RemoveServerRequest<>(newServerId)).get();
    }

    public AbdicateLeadershipResponse deposeLeader() throws ExecutionException, InterruptedException {
        return sendClientRequest(new AbdicateLeadershipRequest()).get();
    }
}
