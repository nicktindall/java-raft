package au.id.tindall.distalg.raft.serverstates;

import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.driver.ElectionScheduler;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.rpc.server.AppendEntriesRequest;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;
import java.util.Optional;

import static au.id.tindall.distalg.raft.serverstates.Result.complete;
import static au.id.tindall.distalg.raft.serverstates.ServerStateType.FOLLOWER;
import static java.lang.Math.min;
import static java.util.Optional.empty;
import static org.apache.logging.log4j.LogManager.getLogger;

public class Follower<ID extends Serializable> extends ServerState<ID> {

    private static final Logger LOGGER = getLogger();

    private final Term followerForTerm;
    private final ID leaderForTerm;
    private final ElectionScheduler<ID> electionScheduler;

    public Follower(Term currentTerm, ID votedFor, Log log, Cluster<ID> cluster, ServerStateFactory<ID> serverStateFactory, ID currentLeader, ElectionScheduler<ID> electionScheduler) {
        super(currentTerm, votedFor, log, cluster, serverStateFactory, currentLeader);
        this.followerForTerm = currentTerm;
        this.leaderForTerm = currentLeader;
        this.electionScheduler = electionScheduler;
    }

    @Override
    public void enterState() {
        LOGGER.debug("Server entering Follower state");
        electionScheduler.startTimeouts();
    }

    @Override
    public void leaveState() {
        electionScheduler.stopTimeouts();
    }

    @Override
    protected Result<ID> handle(AppendEntriesRequest<ID> appendEntriesRequest) {
        if (appendEntriesRequest.getTerm().isGreaterThan(followerForTerm)) {
            throw new IllegalStateException("Received a request from a future term! this should never happen");
        }

        if (messageIsStale(appendEntriesRequest)) {
            getCluster().sendAppendEntriesResponse(getCurrentTerm(), appendEntriesRequest.getLeaderId(), false, empty());
            return complete(this);
        }

        if (!appendEntriesRequest.getSource().equals(leaderForTerm)) {
            LOGGER.warn("Got an append entries request from someone other than the leader?!");
            getCluster().sendAppendEntriesResponse(getCurrentTerm(), appendEntriesRequest.getLeaderId(), false, empty());
            return complete(this);
        }

        electionScheduler.resetTimeout();

        if (appendEntriesRequest.getPrevLogIndex() > 0 &&
                !getLog().containsPreviousEntry(appendEntriesRequest.getPrevLogIndex(), appendEntriesRequest.getPrevLogTerm())) {
            getCluster().sendAppendEntriesResponse(getCurrentTerm(), appendEntriesRequest.getLeaderId(), false, empty());
            return complete(this);
        }

        getLog().appendEntries(appendEntriesRequest.getPrevLogIndex(), appendEntriesRequest.getEntries());
        getLog().advanceCommitIndex(min(getLog().getLastLogIndex(), appendEntriesRequest.getLeaderCommit()));
        int indexOfLastEntryAppended = appendEntriesRequest.getPrevLogIndex() + appendEntriesRequest.getEntries().size();
        getCluster().sendAppendEntriesResponse(appendEntriesRequest.getTerm(), appendEntriesRequest.getLeaderId(), true, Optional.of(indexOfLastEntryAppended));
        return complete(this);
    }

    @Override
    public ServerStateType getServerStateType() {
        return FOLLOWER;
    }
}
