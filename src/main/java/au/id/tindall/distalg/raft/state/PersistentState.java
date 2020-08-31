package au.id.tindall.distalg.raft.state;

import au.id.tindall.distalg.raft.log.Term;
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
}
