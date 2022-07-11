package au.id.tindall.distalg.raft.snapshotting;

import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.ConfigurationEntry;
import au.id.tindall.distalg.raft.state.PersistentState;
import au.id.tindall.distalg.raft.state.Snapshot;
import au.id.tindall.distalg.raft.statemachine.StateMachine;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;

import static org.apache.logging.log4j.LogManager.getLogger;

public class Snapshotter<ID extends Serializable> {

    private static final Logger LOGGER = getLogger();

    private final Log log;
    private final StateMachine stateMachine;
    private final PersistentState<ID> persistentState;
    private final SnapshotHeuristic snapshotHeuristic;

    private ConfigurationEntry lastConfigurationEntry;

    public Snapshotter(Log log, StateMachine stateMachine, PersistentState<ID> persistentState, SnapshotHeuristic snapshotHeuristic) {
        this.log = log;
        this.stateMachine = stateMachine;
        this.persistentState = persistentState;
        this.snapshotHeuristic = snapshotHeuristic;
    }

    public void createSnapshotIfReady(int lastIndex, Term lastTerm) {
        if (snapshotHeuristic.shouldCreateSnapshot(log, stateMachine, persistentState.getCurrentSnapshot().orElse(null))) {
            LOGGER.warn("Creating snapshot to index: {}, {}", lastIndex, lastTerm);
            byte[] snapshot = stateMachine.createSnapshot();
            try (final Snapshot nextSnapshot = persistentState.createNextSnapshot(lastIndex, lastTerm, lastConfigurationEntry)) {
                nextSnapshot.writeBytes(0, snapshot);
                nextSnapshot.finalise();
                persistentState.promoteNextSnapshot();
            } catch (IOException e) {
                LOGGER.error("Error creating snapshot", e);
            }
        }
    }

    public void logConfigurationEntry(ConfigurationEntry configurationEntry) {
        this.lastConfigurationEntry = configurationEntry;
    }
}
