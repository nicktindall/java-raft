package au.id.tindall.distalg.raft.state;

import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.ConfigurationEntry;
import au.id.tindall.distalg.raft.log.storage.LogStorage;

import java.io.IOException;
import java.io.Serializable;
import java.util.Optional;

public interface PersistentState<ID extends Serializable> {

    ID getId();

    void setCurrentTerm(Term term);

    Term getCurrentTerm();

    void setVotedFor(ID votedFor);

    Optional<ID> getVotedFor();

    LogStorage getLogStorage();

    void setCurrentSnapshot(Snapshot snapshot) throws IOException;

    Optional<Snapshot> getCurrentSnapshot();

    Snapshot createSnapshot(int lastIndex, Term lastTerm, ConfigurationEntry lastConfig) throws IOException;

    Snapshot createSnapshot(int lastIndex, Term lastTerm, ConfigurationEntry lastConfig, int snapshotOffset) throws IOException;

    void addSnapshotInstalledListener(SnapshotInstalledListener listener);

    void initialize();
}

