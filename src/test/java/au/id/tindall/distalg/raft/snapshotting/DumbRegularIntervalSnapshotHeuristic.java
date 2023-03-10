package au.id.tindall.distalg.raft.snapshotting;

import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.state.Snapshot;
import au.id.tindall.distalg.raft.statemachine.StateMachine;
import org.apache.logging.log4j.Logger;

import static org.apache.logging.log4j.status.StatusLogger.getLogger;

/**
 * This just triggers snapshot creation whenever there's 1,000 committed
 * log entries since the last snapshot
 */
public class DumbRegularIntervalSnapshotHeuristic implements SnapshotHeuristic {
    private static final Logger LOGGER = getLogger();
    private static final int LOG_INTERVAL = 1_000;

    @Override
    public boolean shouldCreateSnapshot(Log log, StateMachine stateMachine, Snapshot currentSnapshot) {
        final boolean shouldCreateSnapshot = (currentSnapshot == null && log.getCommitIndex() >= LOG_INTERVAL
                || currentSnapshot != null && log.getCommitIndex() - currentSnapshot.getLastIndex() > LOG_INTERVAL);
        if (shouldCreateSnapshot) {
            LOGGER.info("Snapshotting at " + log.getCommitIndex());
        }
        return shouldCreateSnapshot;
    }
}
