package au.id.tindall.distalg.raft.serverstates.clustermembership;

import au.id.tindall.distalg.raft.cluster.Configuration;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.rpc.clustermembership.RemoveServerResponse;
import au.id.tindall.distalg.raft.state.PersistentState;

import java.io.Serializable;

public class RemoveServer<ID extends Serializable> extends MembershipChange<ID, RemoveServerResponse> {

    RemoveServer(Log log, Configuration<ID> configuration, PersistentState<ID> persistentState, ID serverId) {
        super(log, configuration, persistentState, serverId);
    }

    @Override
    void start() {
        finishedAtIndex = removeServerFromConfig(serverId);
    }

    @Override
    protected RemoveServerResponse entryCommittedInternal(int index) {
        if (finishedAtIndex == index) {
            return RemoveServerResponse.OK;
        }
        return null;
    }
}
