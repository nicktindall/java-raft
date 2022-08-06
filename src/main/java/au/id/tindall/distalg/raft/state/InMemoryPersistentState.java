package au.id.tindall.distalg.raft.state;

import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.ConfigurationEntry;
import au.id.tindall.distalg.raft.log.storage.InMemoryLogStorage;
import au.id.tindall.distalg.raft.log.storage.LogStorage;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

public class InMemoryPersistentState<ID extends Serializable> implements PersistentState<ID> {

    private final ID id;
    private final LogStorage logStorage;
    private final AtomicReference<Term> currentTerm;
    private final AtomicReference<ID> votedFor;
    private InMemorySnapshot currentSnapshot;
    private List<SnapshotInstalledListener> snapshotInstalledListeners;

    public InMemoryPersistentState(ID id) {
        this.id = id;
        this.logStorage = new InMemoryLogStorage();
        this.currentTerm = new AtomicReference<>(new Term(0));
        this.votedFor = new AtomicReference<>();
        this.snapshotInstalledListeners = new ArrayList<>();
    }

    @Override
    public ID getId() {
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
    public void setVotedFor(ID votedFor) {
        this.votedFor.set(votedFor);
    }

    @Override
    public Optional<ID> getVotedFor() {
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
        snapshot.snapshotOffset(snapshotOffset);
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
