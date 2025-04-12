package au.id.tindall.distalg.raft.serverstates.clustermembership;

import au.id.tindall.distalg.raft.cluster.Configuration;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.replication.ReplicationManager;
import au.id.tindall.distalg.raft.rpc.clustermembership.RemoveServerResponse;
import au.id.tindall.distalg.raft.state.PersistentState;

import java.time.Instant;
import java.util.function.Supplier;

public class RemoveServer<I> extends MembershipChange<I, RemoveServerResponse<I>> {

    RemoveServer(Log log, Configuration<I> configuration, PersistentState<I> persistentState, ReplicationManager<I> replicationManager, I serverId, Supplier<Instant> timeSource) {
        super(log, configuration, persistentState, replicationManager, serverId, timeSource);
    }

    @Override
    protected void onStart() {
        finishedAtIndex = removeServerFromConfig(serverId);
    }

    @Override
    protected RemoveServerResponse<I> entryCommittedInternal(int index) {
        if (finishedAtIndex == index) {
            replicationManager.stopReplicatingTo(serverId);
            return RemoveServerResponse.getOK();
        }
        return null;
    }

    @Override
    protected RemoveServerResponse<I> timeoutIfSlow() {
        // Removes don't time out
        return null;
    }

    @Override
    protected RemoveServerResponse<I> matchIndexAdvancedInternal(int lastAppendedIndex) {
        // Do nothing
        return null;
    }

    @Override
    public void close() {
        responseFuture.complete(RemoveServerResponse.getNotLeader());
    }
}
