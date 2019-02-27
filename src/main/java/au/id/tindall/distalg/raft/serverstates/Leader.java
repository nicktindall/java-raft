package au.id.tindall.distalg.raft.serverstates;

import static au.id.tindall.distalg.raft.serverstates.Result.complete;
import static au.id.tindall.distalg.raft.serverstates.ServerStateType.LEADER;
import static java.util.Collections.unmodifiableMap;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.replication.LogReplicator;
import au.id.tindall.distalg.raft.rpc.AppendEntriesResponse;

public class Leader<ID extends Serializable> extends ServerState<ID> {

    private final Map<ID, LogReplicator<ID>> replicators;

    public Leader(ID id, Term currentTerm, Log log, Cluster<ID> cluster) {
        super(id, currentTerm, null, log, cluster);
        replicators = createReplicators();
    }

    @Override
    protected Result<ID> handle(AppendEntriesResponse<ID> appendEntriesResponse) {
        if (messageIsStale(appendEntriesResponse)) {
            return complete(this);
        } else {
            return handleCurrentAppendResponse(appendEntriesResponse);
        }
    }

    @Override
    public ServerStateType getServerStateType() {
        return LEADER;
    }

    private Result<ID> handleCurrentAppendResponse(AppendEntriesResponse<ID> appendEntriesResponse) {
        ID remoteServerId = appendEntriesResponse.getSource();
        if (appendEntriesResponse.isSuccess()) {
            int lastAppendedIndex = appendEntriesResponse.getAppendedIndex()
                    .orElseThrow(() -> new IllegalStateException("Append entries response was success with no appendedIndex"));
            replicators.get(remoteServerId).logSuccessResponse(lastAppendedIndex);
        } else {
            replicators.get(remoteServerId).logFailedResponse();
        }
        return complete(this);
    }

    public void sendHeartbeatMessage() {
        getReplicators().values()
                .forEach(replicator -> replicator.sendAppendEntriesRequest(getCurrentTerm(), getLog(), getCommitIndex()));
    }

    private Map<ID, LogReplicator<ID>> createReplicators() {
        int defaultNextIndex = getLog().getLastLogIndex() + 1;
        return new HashMap<>(getCluster().getMemberIds().stream()
                .collect(toMap(identity(), id -> new LogReplicator<>(getId(), getCluster(), id, defaultNextIndex))));
    }

    public Map<ID, LogReplicator<ID>> getReplicators() {
        return unmodifiableMap(replicators);
    }
}
