package au.id.tindall.distalg.raft.state;

import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.ConfigurationEntry;
import au.id.tindall.distalg.raft.log.storage.InMemoryLogStorage;
import au.id.tindall.distalg.raft.log.storage.LogStorage;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.logging.log4j.LogManager.getLogger;

public class InMemoryPersistentState<I> implements PersistentState<I> {

    private static final Logger LOGGER = getLogger();

    private final I id;
    private final LogStorage logStorage;
    private final AtomicReference<Term> currentTerm;
    private final AtomicReference<I> votedFor;
    private InMemorySnapshot currentSnapshot;
    private List<SnapshotInstalledListener> snapshotInstalledListeners;

    public InMemoryPersistentState(I id) {
        this.id = id;
        this.logStorage = new InMemoryLogStorage();
        this.currentTerm = new AtomicReference<>(new Term(0));
        this.votedFor = new AtomicReference<>();
        this.snapshotInstalledListeners = new ArrayList<>();
    }

    @Override
    public I getId() {
        return id;
    }

    @Override
    public void setCurrentTerm(Term term) {
        Term currentTerm = this.currentTerm.get();
        if (term.isLessThan(currentTerm)) {
            throw new IllegalArgumentException("Term increases monotonically");
        }
        if (term.isGreaterThan(currentTerm)) {
            this.votedFor.set(null);
            this.currentTerm.set(term);
        }
    }

    @Override
    public Term getCurrentTerm() {
        return currentTerm.get();
    }

    @Override
    public void setVotedFor(I votedFor) {
        this.votedFor.set(votedFor);
    }

    @Override
    public Optional<I> getVotedFor() {
        return Optional.ofNullable(votedFor.get());
    }

    @Override
    public LogStorage getLogStorage() {
        return logStorage;
    }

    @Override
    public Optional<Snapshot> getCurrentSnapshot() {
        return Optional.ofNullable(currentSnapshot);
    }

    @Override
    public void setCurrentSnapshot(Snapshot nextSnapshot) {
        // no point installing a snapshot if we've already gone past that point
        if (currentSnapshot != null && nextSnapshot.getLastIndex() <= currentSnapshot.getLastIndex()) {
            LOGGER.debug("Not installing snapshot that would not advance us (currentSnapshot.getLastIndex() == {}, nextSnapshot.getLastLogIndex() == {}",
                    currentSnapshot.getLastIndex(), nextSnapshot.getLastIndex());
            return;
        }
        currentSnapshot = (InMemorySnapshot) nextSnapshot;
        logStorage.installSnapshot(currentSnapshot);
        for (SnapshotInstalledListener listener : snapshotInstalledListeners) {
            listener.onSnapshotInstalled(currentSnapshot);
        }
    }

    @Override
    public Snapshot createSnapshot(int lastIndex, Term lastTerm, ConfigurationEntry lastConfig) {
        return new InMemorySnapshot(lastIndex, lastTerm, lastConfig);
    }

    @Override
    public Snapshot createSnapshot(int lastIndex, Term lastTerm, ConfigurationEntry lastConfig, int snapshotOffset) {
        final Snapshot snapshot = createSnapshot(lastIndex, lastTerm, lastConfig);
        snapshot.setSnapshotOffset(snapshotOffset);
        return snapshot;
    }

    @Override
    public void addSnapshotInstalledListener(SnapshotInstalledListener listener) {
        snapshotInstalledListeners.add(listener);
    }

    @Override
    public void initialize() {
        // Do nothing
    }
}
