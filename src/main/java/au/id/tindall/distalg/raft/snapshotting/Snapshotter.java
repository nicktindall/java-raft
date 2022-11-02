package au.id.tindall.distalg.raft.snapshotting;

import au.id.tindall.distalg.raft.client.sessions.ClientSessionStore;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.ConfigurationEntry;
import au.id.tindall.distalg.raft.state.PersistentState;
import au.id.tindall.distalg.raft.state.Snapshot;
import au.id.tindall.distalg.raft.statemachine.StateMachine;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

import static org.apache.logging.log4j.LogManager.getLogger;

public class Snapshotter {

    private static final Logger LOGGER = getLogger();

    private final Log log;
    private final StateMachine stateMachine;
    private final PersistentState<?> persistentState;
    private final SnapshotHeuristic snapshotHeuristic;
    private final ClientSessionStore clientSessionStore;

    private ConfigurationEntry lastConfigurationEntry;

    public Snapshotter(Log log, ClientSessionStore clientSessionStore, StateMachine stateMachine, PersistentState<?> persistentState, SnapshotHeuristic snapshotHeuristic) {
        this.log = log;
        this.clientSessionStore = clientSessionStore;
        this.stateMachine = stateMachine;
        this.persistentState = persistentState;
        this.snapshotHeuristic = snapshotHeuristic;
    }

    public void createSnapshotIfReady(int committedIndex) {
        if (snapshotHeuristic.shouldCreateSnapshot(log, stateMachine, persistentState.getCurrentSnapshot().orElse(null))) {
            byte[] snapshot = stateMachine.createSnapshot();
            Term termAtIndex = persistentState.getLogStorage().getEntry(committedIndex).getTerm();
            LOGGER.debug("Creating snapshot to index={}, term={}, length={}", committedIndex, termAtIndex, snapshot.length);
            try (final Snapshot nextSnapshot = persistentState.createSnapshot(committedIndex, termAtIndex, lastConfigurationEntry)) {
                final byte[] chunk = clientSessionStore.serializeSessions();
                LOGGER.debug("Serialised sessions size = {}", chunk.length);
                final int startOfSnapshot = nextSnapshot.writeBytes(0, chunk);
                nextSnapshot.finaliseSessions();
                nextSnapshot.writeBytes(startOfSnapshot, snapshot);
                nextSnapshot.finalise();
                persistentState.setCurrentSnapshot(nextSnapshot);
                nextSnapshot.delete();
            } catch (IOException e) {
                LOGGER.error("Error creating snapshot", e);
            }
        }
    }

    public void logConfigurationEntry(ConfigurationEntry configurationEntry) {
        this.lastConfigurationEntry = configurationEntry;
    }
}
