package au.id.tindall.distalg.raft.serverstates;

import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.elections.ElectionScheduler;
import au.id.tindall.distalg.raft.log.EntryStatus;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.rpc.server.AppendEntriesRequest;
import au.id.tindall.distalg.raft.rpc.snapshots.InstallSnapshotRequest;
import au.id.tindall.distalg.raft.state.PersistentState;
import au.id.tindall.distalg.raft.state.Snapshot;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Optional;

import static au.id.tindall.distalg.raft.serverstates.Result.complete;
import static au.id.tindall.distalg.raft.serverstates.ServerStateType.FOLLOWER;
import static au.id.tindall.distalg.raft.util.HexUtil.hexDump;
import static java.lang.Math.min;
import static java.util.Optional.empty;
import static org.apache.logging.log4j.LogManager.getLogger;

public class Follower<ID extends Serializable> extends ServerState<ID> {

    private static final Logger LOGGER = getLogger();

    private final ElectionScheduler electionScheduler;
    private Snapshot receivingSnapshot;

    public Follower(PersistentState<ID> persistentState, Log log, Cluster<ID> cluster, ServerStateFactory<ID> serverStateFactory, ID currentLeader, ElectionScheduler electionScheduler) {
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
            cluster.sendAppendEntriesResponse(persistentState.getCurrentTerm(), appendEntriesRequest.getLeaderId(), false, empty());
            return complete(this);
        }

        if (!appendEntriesRequest.getSource().equals(currentLeader)) {
            LOGGER.warn("Got an append entries request from someone other than the leader?!");
            cluster.sendAppendEntriesResponse(persistentState.getCurrentTerm(), appendEntriesRequest.getLeaderId(), false, empty());
            return complete(this);
        }

        electionScheduler.resetTimeout();

        if (appendEntriesRequest.getPrevLogIndex() > 0) {
            final EntryStatus entryStatus = log.hasEntry(appendEntriesRequest.getPrevLogIndex());
            switch (entryStatus) {
                case AfterEnd:
                    LOGGER.warn("Couldn't append entry: appendPrevIndex={}, log.getPrevIndex={}, appendPrevTerm={}, log.hasEntry={}", appendEntriesRequest.getPrevLogIndex(), log.getPrevIndex(),
                            appendEntriesRequest.getPrevLogTerm(), log.hasEntry(appendEntriesRequest.getPrevLogIndex()));
                    cluster.sendAppendEntriesResponse(persistentState.getCurrentTerm(), appendEntriesRequest.getLeaderId(), false, Optional.of(log.getLastLogIndex() + 1));
                    return complete(this);
                case BeforeStart:
                    if (log.getPrevIndex() != appendEntriesRequest.getPrevLogIndex() || !log.getPrevTerm().equals(appendEntriesRequest.getPrevLogTerm())) {
                        LOGGER.warn("Ignoring append entry: appendPrevIndex={}, appendPrevTerm={}, log.getPrevIndex={}", appendEntriesRequest.getPrevLogIndex(), appendEntriesRequest.getPrevLogTerm(),
                                log.getPrevIndex());
                        cluster.sendAppendEntriesResponse(persistentState.getCurrentTerm(), appendEntriesRequest.getLeaderId(), false, Optional.of(Math.max(log.getLastLogIndex() + 1, log.getPrevIndex() + 1)));
                        return complete(this);
                    }
                    break;
                case Present:
                    if (!log.containsPreviousEntry(appendEntriesRequest.getPrevLogIndex(), appendEntriesRequest.getPrevLogTerm())) {
                        LOGGER.warn("Couldn't append entry: appendPrevIndex={}, log.getPrevIndex={}, appendPrevTerm={}, log.hasEntry={}", appendEntriesRequest.getPrevLogIndex(), log.getPrevIndex(),
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
            cluster.sendInstallSnapshotResponse(persistentState.getCurrentTerm(), installSnapshotRequest.getLeaderId(), false,
                    installSnapshotRequest.getLastIndex(), installSnapshotRequest.getOffset() + installSnapshotRequest.getData().length);
            return complete(this);
        }

        if (!installSnapshotRequest.getSource().equals(currentLeader)) {
            LOGGER.warn("Got an install snapshot request from someone other than the leader?!");
            cluster.sendAppendEntriesResponse(persistentState.getCurrentTerm(), installSnapshotRequest.getLeaderId(), false, empty());
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
                LOGGER.warn("Got an InstallSnapshotRequest late, there is no current snapshot, ignoring (offset={}, lastIndex={})",
                        installSnapshotRequest.getOffset(), installSnapshotRequest.getLastIndex());
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
                installSnapshotRequest.getLastIndex(), installSnapshotRequest.getOffset() + installSnapshotRequest.getData().length);
    }

    private void updateAndPromoteSnapshot(InstallSnapshotRequest<ID> installSnapshotRequest) {
        int bytesWritten = receivingSnapshot.writeBytes(installSnapshotRequest.getOffset(), installSnapshotRequest.getData());
        if (installSnapshotRequest.isDone()) {
            receivingSnapshot.finalise();
            ByteBuffer endOfFirstChunkBytes = ByteBuffer.allocate(50);
            ByteBuffer endBytes = ByteBuffer.allocate(50);
            receivingSnapshot.readInto(endOfFirstChunkBytes, 4050);
            receivingSnapshot.readInto(endBytes, (int) receivingSnapshot.getLength() - 50);
            LOGGER.debug("Received snapshot index={}, term={}, length={}, endOfFirstChunk={}, end={}", receivingSnapshot.getLastIndex(), receivingSnapshot.getLastTerm(), receivingSnapshot.getLength(),
                    hexDump(endOfFirstChunkBytes.array()), hexDump(endBytes.array()));
            persistentState.setCurrentSnapshot(receivingSnapshot);
            receivingSnapshot.delete();
            receivingSnapshot = null;
        }
        cluster.sendInstallSnapshotResponse(persistentState.getCurrentTerm(), installSnapshotRequest.getLeaderId(), true,
                installSnapshotRequest.getLastIndex(), installSnapshotRequest.getOffset() + bytesWritten - 1);
    }

    @Override
    public ServerStateType getServerStateType() {
        return FOLLOWER;
    }
}
