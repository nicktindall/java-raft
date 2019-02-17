package au.id.tindall.distalg.raft.serverstates;

import static au.id.tindall.distalg.raft.serverstates.Result.complete;
import static au.id.tindall.distalg.raft.serverstates.ServerStateType.FOLLOWER;
import static java.lang.Math.min;
import static java.util.Optional.empty;

import java.io.Serializable;
import java.util.Optional;

import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.rpc.AppendEntriesRequest;
import au.id.tindall.distalg.raft.rpc.AppendEntriesResponse;

public class Follower<ID extends Serializable> extends ServerState<ID> {

    public Follower(ID id, Term currentTerm, ID votedFor, Log log, Cluster<ID> cluster) {
        super(id, currentTerm, votedFor, log, cluster);
    }

    @Override
    protected Result<ID> handle(AppendEntriesRequest<ID> appendEntriesRequest) {
        if (messageIsStale(appendEntriesRequest)) {
            getCluster().send(new AppendEntriesResponse<>(getCurrentTerm(), getId(), appendEntriesRequest.getLeaderId(), false, empty()));
            return complete(this);
        }

        if (appendEntriesRequest.getPrevLogIndex() > 0 &&
                !getLog().containsPreviousEntry(appendEntriesRequest.getPrevLogIndex(), appendEntriesRequest.getPrevLogTerm())) {
            getCluster().send(new AppendEntriesResponse<>(getCurrentTerm(), getId(), appendEntriesRequest.getLeaderId(), false, empty()));
            return complete(this);
        }

        getLog().appendEntries(appendEntriesRequest.getPrevLogIndex(), appendEntriesRequest.getEntries());
        setCommitIndex(min(getLog().getLastLogIndex(), appendEntriesRequest.getLeaderCommit()));
        int indexOfLastEntryAppended = appendEntriesRequest.getPrevLogIndex() + appendEntriesRequest.getEntries().size();
        getCluster().send(new AppendEntriesResponse<>(appendEntriesRequest.getTerm(), getId(), appendEntriesRequest.getLeaderId(), true, Optional.of(indexOfLastEntryAppended)));
        return complete(this);
    }

    @Override
    public ServerStateType getServerStateType() {
        return FOLLOWER;
    }
}
