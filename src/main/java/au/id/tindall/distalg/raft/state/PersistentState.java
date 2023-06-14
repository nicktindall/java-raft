package au.id.tindall.distalg.raft.state;

import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.ConfigurationEntry;
import au.id.tindall.distalg.raft.log.storage.LogStorage;

import java.io.IOException;
import java.io.Serializable;
import java.util.Optional;

public interface PersistentState<I extends Serializable> {

    I getId();

    void setCurrentTerm(Term term);

    Term getCurrentTerm();

    void setVotedFor(I votedFor);

    Optional<I> getVotedFor();

    default void setCurrentTermAndVotedFor(Term term, I votedFor) {
        setCurrentTerm(term);
        setVotedFor(votedFor);
    }

    LogStorage getLogStorage();

    void setCurrentSnapshot(Snapshot snapshot) throws IOException;

    Optional<Snapshot> getCurrentSnapshot();

    Snapshot createSnapshot(int lastIndex, Term lastTerm, ConfigurationEntry lastConfig) throws IOException;

    Snapshot createSnapshot(int lastIndex, Term lastTerm, ConfigurationEntry lastConfig, int snapshotOffset) throws IOException;

    void addSnapshotInstalledListener(SnapshotInstalledListener listener);

    void initialize();
}

