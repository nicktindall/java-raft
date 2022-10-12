package au.id.tindall.distalg.raft.serverstates.clustermembership;

import au.id.tindall.distalg.raft.cluster.Configuration;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.replication.ReplicationManager;
import au.id.tindall.distalg.raft.rpc.clustermembership.RemoveServerResponse;
import au.id.tindall.distalg.raft.state.PersistentState;

import java.io.Serializable;
import java.time.Instant;
import java.util.function.Supplier;

public class RemoveServer<ID extends Serializable> extends MembershipChange<ID, RemoveServerResponse> {

    RemoveServer(Log log, Configuration<ID> configuration, PersistentState<ID> persistentState, ReplicationManager<ID> replicationManager, ID serverId, Supplier<Instant> timeSource) {
        super(log, configuration, persistentState, replicationManager, serverId, timeSource);
    }

    @Override
    protected void onStart() {
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

    @Override
    protected RemoveServerResponse timeoutIfSlow() {
        // Removes don't time out
        return null;
    }

    @Override
    protected RemoveServerResponse matchIndexAdvancedInternal(int lastAppendedIndex) {
        // Do nothing
        return null;
    }

    @Override
    public void close() {
        responseFuture.complete(RemoveServerResponse.NOT_LEADER);
    }
}
