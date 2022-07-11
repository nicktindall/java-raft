package au.id.tindall.distalg.raft.state;

import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.ConfigurationEntry;
import au.id.tindall.distalg.raft.log.storage.LogStorage;

import java.io.Serializable;
import java.util.Optional;

public interface PersistentState<ID extends Serializable> {

    ID getId();

    void setCurrentTerm(Term term);

    Term getCurrentTerm();

    void setVotedFor(ID votedFor);

    Optional<ID> getVotedFor();

    LogStorage getLogStorage();

    /**
     * The latest cluster membership configuration up through prevIndex
     *
     * @return the prevConfig
     */
    default ConfigurationEntry getPrevConfig() {
        return null;
    }

    void promoteNextSnapshot();

    default Optional<Snapshot> getCurrentSnapshot() {
        return Optional.empty();
    }

    Optional<Snapshot> getNextSnapshot();

    Snapshot createNextSnapshot(int lastIndex, Term lastTerm, ConfigurationEntry lastConfig);

    void addSnapshotInstalledListener(SnapshotInstalledListener listener);

}

