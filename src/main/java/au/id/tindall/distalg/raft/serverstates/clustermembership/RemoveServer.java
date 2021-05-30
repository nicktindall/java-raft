package au.id.tindall.distalg.raft.serverstates.clustermembership;

import au.id.tindall.distalg.raft.cluster.Configuration;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.replication.ReplicationManager;
import au.id.tindall.distalg.raft.rpc.clustermembership.RemoveServerResponse;
import au.id.tindall.distalg.raft.state.PersistentState;

import java.io.Serializable;

public class RemoveServer<ID extends Serializable> extends MembershipChange<ID, RemoveServerResponse> {

    private final ReplicationManager<ID> replicationManager;

    RemoveServer(Log log, Configuration<ID> configuration, PersistentState<ID> persistentState, ReplicationManager<ID> replicationManager, ID serverId) {
        super(log, configuration, persistentState, serverId);
        this.replicationManager = replicationManager;
    }

    @Override
    void start() {
        finishedAtIndex = removeServerFromConfig(serverId);
    }

    @Override
    protected RemoveServerResponse entryCommittedInternal(int index) {
        if (finishedAtIndex == index) {
            replicationManager.stopReplicatingTo(serverId);
            return RemoveServerResponse.OK;
        }
        return null;
    }
}
