package au.id.tindall.distalg.raft.replication;

import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.state.PersistentState;
import au.id.tindall.distalg.raft.state.Snapshot;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Optional;

import static org.apache.logging.log4j.LogManager.getLogger;

public class SnapshotReplicator<ID extends Serializable> implements StateReplicator<ID> {

    private static final Logger LOGGER = getLogger();

    private static final int CHUNK_SIZE = 1 << 12;
    private final ByteBuffer buffer = ByteBuffer.allocate(CHUNK_SIZE);

    private final Term term;
    private final Cluster<ID> cluster;
    private final ID followerId;
    private final PersistentState<ID> persistentState;

    private int currentSnapshotLastIndex = -1;
    private Term currentSnapshotLastTerm = null;
    private int lastOffsetConfirmed = 0;

    public SnapshotReplicator(Term term, Cluster<ID> cluster, ID followerId, PersistentState<ID> persistentState) {
        this.term = term;
        this.cluster = cluster;
        this.followerId = followerId;
        this.persistentState = persistentState;
    }

    @Override
    public void sendNextReplicationMessage() {
        final Optional<Snapshot> currentSnapshot = persistentState.getCurrentSnapshot();
        currentSnapshot.ifPresentOrElse(snapshot -> {
            if (sendingANewSnapshot(snapshot)) {
                resetSendingState(snapshot);
            }
            final int nextOffset = lastOffsetConfirmed + 1;
            snapshot.readInto(buffer, nextOffset);
            cluster.sendInstallSnapshotRequest(term, followerId, snapshot.getLastIndex(), snapshot.getLastTerm(),
                    snapshot.getLastConfig(), nextOffset, buffer, buffer.hasRemaining());
        }, () -> {
            LOGGER.warn("Attempted to send snapshot but there is no current snapshot");
        });
    }

    @Override
    public void logSuccessResponse(int lastAppendedIndex) {
        // Do nothing!
    }

    @Override
    public int getMatchIndex() {
        // Assume remote has no state
        return 0;
    }

    @Override
    public int getNextIndex() {
        // Assume remote has no state
        return 0;
    }

    @Override
    public void logFailedResponse(Integer followerLastLogIndex) {
        // Do nothing!
    }

    private void resetSendingState(Snapshot snapshot) {
        currentSnapshotLastIndex = snapshot.getLastIndex();
        currentSnapshotLastTerm = snapshot.getLastTerm();
        lastOffsetConfirmed = 0;
    }

    private boolean sendingANewSnapshot(Snapshot currentSnapshot) {
        return currentSnapshotLastTerm == null
                || currentSnapshot.getLastIndex() != currentSnapshotLastIndex
                || !currentSnapshot.getLastTerm().equals(currentSnapshotLastTerm);
    }
}
