package au.id.tindall.distalg.raft.state;

import au.id.tindall.distalg.raft.log.Term;

import java.io.Serializable;
import java.util.Optional;

public interface PersistentState<ID extends Serializable> {

    ID getId();

    void setCurrentTerm(Term currentTerm);

    Term getCurrentTerm();

    void setVotedFor(ID votedFor);

    Optional<ID> getVotedFor();
}
