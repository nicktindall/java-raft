package au.id.tindall.distalg.raft.serverstates;

import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.elections.ElectionScheduler;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.rpc.server.AppendEntriesRequest;
import au.id.tindall.distalg.raft.state.PersistentState;
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

    private final ElectionScheduler<ID> electionScheduler;

    public Follower(PersistentState<ID> persistentState, Log log, Cluster<ID> cluster, ServerStateFactory<ID> serverStateFactory, ID currentLeader, ElectionScheduler<ID> electionScheduler) {
        super(persistentState, log, cluster, serverStateFactory, currentLeader);
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
        if (appendEntriesRequest.getTerm().isGreaterThan(persistentState.getCurrentTerm())) {
            throw new IllegalStateException("Received a request from a future term! this should never happen");
        }

        if (messageIsStale(appendEntriesRequest)) {
            cluster.sendAppendEntriesResponse(persistentState.getCurrentTerm(), appendEntriesRequest.getLeaderId(), false, empty());
            return complete(this);
        }

        if (!appendEntriesRequest.getSource().equals(currentLeader)) {
            LOGGER.warn("Got an append entries request from someone other than the leader?!");
            cluster.sendAppendEntriesResponse(persistentState.getCurrentTerm(), appendEntriesRequest.getLeaderId(), false, empty());
            return complete(this);
        }

        electionScheduler.resetTimeout();

        if (appendEntriesRequest.getPrevLogIndex() > 0 &&
                !log.containsPreviousEntry(appendEntriesRequest.getPrevLogIndex(), appendEntriesRequest.getPrevLogTerm())) {
            cluster.sendAppendEntriesResponse(persistentState.getCurrentTerm(), appendEntriesRequest.getLeaderId(), false, Optional.of(log.getLastLogIndex()));
            return complete(this);
        }

        log.appendEntries(appendEntriesRequest.getPrevLogIndex(), appendEntriesRequest.getEntries());
        log.advanceCommitIndex(min(log.getLastLogIndex(), appendEntriesRequest.getLeaderCommit()));
        int indexOfLastEntryAppended = appendEntriesRequest.getPrevLogIndex() + appendEntriesRequest.getEntries().size();
        cluster.sendAppendEntriesResponse(appendEntriesRequest.getTerm(), appendEntriesRequest.getLeaderId(), true, Optional.of(indexOfLastEntryAppended));
        return complete(this);
    }

    @Override
    public ServerStateType getServerStateType() {
        return FOLLOWER;
    }
}
