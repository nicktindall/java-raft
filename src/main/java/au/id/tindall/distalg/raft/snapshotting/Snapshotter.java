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

import static au.id.tindall.distalg.raft.util.HexUtil.hexDump;
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

    public void createSnapshotIfReady(int lastIndex, Term lastTerm) {
        if (snapshotHeuristic.shouldCreateSnapshot(log, stateMachine, persistentState.getCurrentSnapshot().orElse(null))) {
            byte[] snapshot = stateMachine.createSnapshot();
            LOGGER.warn("Creating snapshot to index={}, term={}, length={}, endOfFirstChunk={}, end={}", lastIndex, lastTerm, snapshot.length,
                    hexDump(snapshot, 4050, 50), hexDump(snapshot, snapshot.length - 50, 50));
            try (final Snapshot nextSnapshot = persistentState.createSnapshot(lastIndex, lastTerm, lastConfigurationEntry)) {
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
