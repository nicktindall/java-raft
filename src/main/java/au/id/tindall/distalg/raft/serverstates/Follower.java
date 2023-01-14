package au.id.tindall.distalg.raft.serverstates;

import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.elections.ElectionScheduler;
import au.id.tindall.distalg.raft.log.EntryStatus;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.rpc.server.AppendEntriesRequest;
import au.id.tindall.distalg.raft.rpc.snapshots.InstallSnapshotRequest;
import au.id.tindall.distalg.raft.state.PersistentState;
import au.id.tindall.distalg.raft.state.Snapshot;
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

    private final ElectionScheduler electionScheduler;
    private Snapshot receivingSnapshot;
    private Term lastReceivedSnapshotLastTerm;
    private long lastReceivedSnapshotLastIndex;

    public Follower(PersistentState<ID> persistentState, Log log, Cluster<ID> cluster, ServerStateFactory<ID> serverStateFactory, ID currentLeader, ElectionScheduler electionScheduler) {
        super(persistentState, log, cluster, serverStateFactory, currentLeader);
        this.electionScheduler = electionScheduler;
    }

    @Override
    public void enterState() {
        LOGGER.debug("Server entering Follower state (leader={}, term={}, lastIndex={}, lastTerm={})",
                currentLeader, persistentState.getCurrentTerm(), log.getLastLogIndex(), log.getLastLogTerm());
        electionScheduler.startTimeouts();
    }

    @Override
    public void leaveState() {
        electionScheduler.stopTimeouts();
        if (receivingSnapshot != null) {
            receivingSnapshot.delete();
            receivingSnapshot = null;
        }
    }

    @Override
    protected Result<ID> handle(AppendEntriesRequest<ID> appendEntriesRequest) {
        if (appendEntriesRequest.getTerm().isGreaterThan(persistentState.getCurrentTerm())) {
            throw new IllegalStateException("Received a request from a future term! this should never happen");
        }

        if (messageIsStale(appendEntriesRequest)) {
            // we reply using the sender's term or a delayed AppendEntriesRequest can cause the prior leader to follow this node in a leadership transfer scenario
            cluster.sendAppendEntriesResponse(appendEntriesRequest.getTerm(), appendEntriesRequest.getLeaderId(), false, empty());
            return complete(this);
        }

        if (!appendEntriesRequest.getSource().equals(currentLeader)) {
            LOGGER.warn("Got an append entries request from someone other than the leader?! (source={}, leader={}))",
                    appendEntriesRequest.getSource(), currentLeader);
            cluster.sendAppendEntriesResponse(persistentState.getCurrentTerm(), appendEntriesRequest.getLeaderId(), false, empty());
            return complete(this);
        }

        electionScheduler.resetTimeout();

        if (appendEntriesRequest.getPrevLogIndex() > 0) {
            final EntryStatus entryStatus = log.hasEntry(appendEntriesRequest.getPrevLogIndex());
            switch (entryStatus) {
                case AfterEnd:
                    LOGGER.debug("Couldn't append entry: appendPrevIndex={}, log.getPrevIndex={}, log.getLastIndex={},appendPrevTerm={}, log.hasEntry={}",
                            appendEntriesRequest.getPrevLogIndex(), log.getPrevIndex(), log.getLastLogIndex(),
                            appendEntriesRequest.getPrevLogTerm(), log.hasEntry(appendEntriesRequest.getPrevLogIndex()));
                    cluster.sendAppendEntriesResponse(persistentState.getCurrentTerm(), appendEntriesRequest.getLeaderId(), false, Optional.of(log.getLastLogIndex() + 1));
                    return complete(this);
                case BeforeStart:
                    if (log.getPrevIndex() != appendEntriesRequest.getPrevLogIndex() || !log.getPrevTerm().equals(appendEntriesRequest.getPrevLogTerm())) {
                        LOGGER.debug("Ignoring append entry: appendPrevIndex={}, appendPrevTerm={}, log.getPrevIndex={}", appendEntriesRequest.getPrevLogIndex(), appendEntriesRequest.getPrevLogTerm(),
                                log.getPrevIndex());
                        cluster.sendAppendEntriesResponse(persistentState.getCurrentTerm(), appendEntriesRequest.getLeaderId(), false, Optional.of(Math.max(log.getLastLogIndex() + 1, log.getPrevIndex() + 1)));
                        return complete(this);
                    }
                    break;
                case Present:
                    if (!log.containsPreviousEntry(appendEntriesRequest.getPrevLogIndex(), appendEntriesRequest.getPrevLogTerm())) {
                        LOGGER.debug("Couldn't append entry: appendPrevIndex={}, log.getPrevIndex={}, log.getLastIndex={}, appendPrevTerm={}, log.hasEntry={}",
                                appendEntriesRequest.getPrevLogIndex(), log.getPrevIndex(), log.getLastLogIndex(),
                                appendEntriesRequest.getPrevLogTerm(), log.hasEntry(appendEntriesRequest.getPrevLogIndex()));
                        cluster.sendAppendEntriesResponse(persistentState.getCurrentTerm(), appendEntriesRequest.getLeaderId(), false, Optional.of(appendEntriesRequest.getPrevLogIndex()));
                        return complete(this);
                    }
                    break;
                default:
                    throw new IllegalStateException("Unexpected entry status: " + entryStatus);
            }
        }

        log.appendEntries(appendEntriesRequest.getPrevLogIndex(), appendEntriesRequest.getEntries());
        log.advanceCommitIndex(min(log.getLastLogIndex(), appendEntriesRequest.getLeaderCommit()));
        int indexOfLastEntryAppended = appendEntriesRequest.getPrevLogIndex() + appendEntriesRequest.getEntries().size();
        cluster.sendAppendEntriesResponse(appendEntriesRequest.getTerm(), appendEntriesRequest.getLeaderId(), true, Optional.of(indexOfLastEntryAppended));
        return complete(this);
    }

    @Override
    protected Result<ID> handle(InstallSnapshotRequest<ID> installSnapshotRequest) {
        if (installSnapshotRequest.getTerm().isGreaterThan(persistentState.getCurrentTerm())) {
            throw new IllegalStateException("Received a request from a future term! this should never happen");
        }

        if (messageIsStale(installSnapshotRequest)) {
            // we reply using the sender's term or a delayed InstallSnapshotRequest can cause the prior leader to follow this node in a leadership transfer scenario
            cluster.sendInstallSnapshotResponse(installSnapshotRequest.getTerm(), installSnapshotRequest.getLeaderId(), false,
                    installSnapshotRequest.getLastIndex(), installSnapshotRequest.getOffset() + installSnapshotRequest.getData().length);
            return complete(this);
        }

        if (!installSnapshotRequest.getSource().equals(currentLeader)) {
            LOGGER.warn("Got an install snapshot request from someone other than the leader?! (source={}, leader={})",
                    installSnapshotRequest.getSource(), currentLeader);
            cluster.sendInstallSnapshotResponse(persistentState.getCurrentTerm(), installSnapshotRequest.getLeaderId(), false, installSnapshotRequest.getLastIndex(),
                    installSnapshotRequest.getOffset() + installSnapshotRequest.getData().length);
            return complete(this);
        }

        electionScheduler.resetTimeout();

        if (installSnapshotRequest.getOffset() == 0) {
            if (receivingSnapshot != null) {
                if (receivingSnapshot.getLastIndex() != installSnapshotRequest.getLastIndex()
                        || !receivingSnapshot.getLastTerm().equals(installSnapshotRequest.getLastTerm())) {
                    LOGGER.debug("Started receiving snapshot starting at lastIndex/term {}/{}", installSnapshotRequest.getLastIndex(), installSnapshotRequest.getLastTerm());
                    receivingSnapshot.delete();
                    receivingSnapshot = persistentState.createSnapshot(installSnapshotRequest.getLastIndex(), installSnapshotRequest.getLastTerm(), installSnapshotRequest.getLastConfig(),
                            installSnapshotRequest.getSnapshotOffset());
                }
            } else {
                receivingSnapshot = persistentState.createSnapshot(installSnapshotRequest.getLastIndex(), installSnapshotRequest.getLastTerm(), installSnapshotRequest.getLastConfig(),
                        installSnapshotRequest.getSnapshotOffset());
            }
        } else {
            if (receivingSnapshot == null) {
                if (installSnapshotRequest.getLastIndex() == lastReceivedSnapshotLastIndex
                        && installSnapshotRequest.getLastTerm().equals(lastReceivedSnapshotLastTerm)) {
                    LOGGER.debug("Got an InstallSnapshotRequest late, acknowledging it");
                    cluster.sendInstallSnapshotResponse(persistentState.getCurrentTerm(), installSnapshotRequest.getLeaderId(), true, installSnapshotRequest.getLastIndex(),
                            installSnapshotRequest.getOffset() + installSnapshotRequest.getData().length);
                } else {
                    LOGGER.warn("Got InstallSnapshotRequest for an unknown snapshot. Ignoring (offset={}, lastIndex={})",
                            installSnapshotRequest.getOffset(), installSnapshotRequest.getLastIndex());
                }
                return complete(this);
            }
        }

        if (receivingSnapshot.getLastIndex() == installSnapshotRequest.getLastIndex()
                && receivingSnapshot.getLastTerm().equals(installSnapshotRequest.getLastTerm())) {
            updateAndPromoteSnapshot(installSnapshotRequest);
        } else {
            sendInstallSnapshotFailedResponse(installSnapshotRequest);
        }
        return complete(this);
    }

    private void sendInstallSnapshotFailedResponse(InstallSnapshotRequest<ID> installSnapshotRequest) {
        cluster.sendInstallSnapshotResponse(persistentState.getCurrentTerm(), installSnapshotRequest.getLeaderId(), false,
                installSnapshotRequest.getLastIndex(), installSnapshotRequest.getOffset() + installSnapshotRequest.getData().length - 1);
    }

    private void updateAndPromoteSnapshot(InstallSnapshotRequest<ID> installSnapshotRequest) {
        int bytesWritten = receivingSnapshot.writeBytes(installSnapshotRequest.getOffset(), installSnapshotRequest.getData());
        if (installSnapshotRequest.isDone()) {
            receivingSnapshot.finalise();
            LOGGER.debug("Received snapshot lastIndex={}, lastTerm={}, length={}", receivingSnapshot.getLastIndex(), receivingSnapshot.getLastTerm(), receivingSnapshot.getLength());
            persistentState.setCurrentSnapshot(receivingSnapshot);
            receivingSnapshot.delete();
            receivingSnapshot = null;
        }
        lastReceivedSnapshotLastIndex = installSnapshotRequest.getLastIndex();
        lastReceivedSnapshotLastTerm = installSnapshotRequest.getLastTerm();
        cluster.sendInstallSnapshotResponse(persistentState.getCurrentTerm(), installSnapshotRequest.getLeaderId(), true,
                installSnapshotRequest.getLastIndex(), installSnapshotRequest.getOffset() + bytesWritten - 1);
    }

    @Override
    public ServerStateType getServerStateType() {
        return FOLLOWER;
    }
}
