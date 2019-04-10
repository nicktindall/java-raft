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
import au.id.tindall.distalg.raft.rpc.server.AppendEntriesRequest;

public class Follower<ID extends Serializable> extends ServerState<ID> {

    public Follower(Term currentTerm, ID votedFor, Log log, Cluster<ID> cluster, ServerStateFactory<ID> serverStateFactory) {
        super(currentTerm, votedFor, log, cluster, serverStateFactory);
    }

    @Override
    protected Result<ID> handle(AppendEntriesRequest<ID> appendEntriesRequest) {
        if (messageIsStale(appendEntriesRequest)) {
            getCluster().sendAppendEntriesResponse(getCurrentTerm(), appendEntriesRequest.getLeaderId(), false, empty());
            return complete(this);
        }

        if (appendEntriesRequest.getPrevLogIndex() > 0 &&
                !getLog().containsPreviousEntry(appendEntriesRequest.getPrevLogIndex(), appendEntriesRequest.getPrevLogTerm())) {
            getCluster().sendAppendEntriesResponse(getCurrentTerm(), appendEntriesRequest.getLeaderId(), false, empty());
            return complete(this);
        }

        getLog().appendEntries(appendEntriesRequest.getPrevLogIndex(), appendEntriesRequest.getEntries());
        getLog().setCommitIndex(min(getLog().getLastLogIndex(), appendEntriesRequest.getLeaderCommit()));
        int indexOfLastEntryAppended = appendEntriesRequest.getPrevLogIndex() + appendEntriesRequest.getEntries().size();
        getCluster().sendAppendEntriesResponse(appendEntriesRequest.getTerm(), appendEntriesRequest.getLeaderId(), true, Optional.of(indexOfLastEntryAppended));
        return complete(this);
    }

    @Override
    public ServerStateType getServerStateType() {
        return FOLLOWER;
    }
}
