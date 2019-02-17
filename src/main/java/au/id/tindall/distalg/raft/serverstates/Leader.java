package au.id.tindall.distalg.raft.serverstates;

import static au.id.tindall.distalg.raft.serverstates.Result.complete;
import static au.id.tindall.distalg.raft.serverstates.ServerStateType.LEADER;
import static java.lang.Math.max;
import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableMap;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.rpc.AppendEntriesRequest;
import au.id.tindall.distalg.raft.rpc.AppendEntriesResponse;

public class Leader<ID extends Serializable> extends ServerState<ID> {

    private final Map<ID, Integer> nextIndices;
    private final Map<ID, Integer> matchIndices;

    public Leader(ID id, Term currentTerm, Log log, Cluster<ID> cluster) {
        super(id, currentTerm, null, log, cluster);
        nextIndices = initialNextIndices();
        matchIndices = initialMatchIndices();
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
            nextIndices.put(remoteServerId, lastAppendedIndex + 1);
            matchIndices.put(remoteServerId, lastAppendedIndex);
        } else {
            nextIndices.put(remoteServerId, max(nextIndices.get(remoteServerId) - 1, 1));
        }
        return complete(this);
    }

    public void sendHeartbeatMessage() {
        getCluster().send(new AppendEntriesRequest<>(getCurrentTerm(), getId(), getLog().getLastLogIndex(), getLog().getLastLogTerm(), emptyList(), getCommitIndex()));
    }

    private Map<ID, Integer> initialMatchIndices() {
        return new HashMap<>(getCluster().getMemberIds().stream()
                .collect(toMap(identity(), id -> 0)));
    }

    private Map<ID, Integer> initialNextIndices() {
        int defaultNextIndex = getLog().getLastLogIndex() + 1;
        return new HashMap<>(getCluster().getMemberIds().stream()
                .collect(toMap(identity(), id -> defaultNextIndex)));
    }

    public Map<ID, Integer> getNextIndices() {
        return unmodifiableMap(nextIndices);
    }

    public Map<ID, Integer> getMatchIndices() {
        return unmodifiableMap(matchIndices);
    }
}
