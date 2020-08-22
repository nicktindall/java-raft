package au.id.tindall.distalg.raft.state;

import au.id.tindall.distalg.raft.log.Term;

import java.io.Serializable;
import java.util.Optional;

public class InMemoryPersistentState<ID extends Serializable> implements PersistentState<ID> {

    private final ID id;
    private Term currentTerm;
    private ID votedFor;

    public InMemoryPersistentState(ID id) {
        this.id = id;
        this.currentTerm = new Term(0);
    }

    @Override
    public ID getId() {
        return id;
    }

    @Override
    public void setCurrentTerm(Term newTerm) {
        if (newTerm.isLessThan(currentTerm)) {
            throw new IllegalArgumentException("Term increases monotonically");
        }
        if (newTerm.isGreaterThan(currentTerm)) {
            this.votedFor = null;
            this.currentTerm = newTerm;
        }
    }

    @Override
    public Term getCurrentTerm() {
        return currentTerm;
    }

    @Override
    public void setVotedFor(ID votedFor) {
        this.votedFor = votedFor;
    }

    @Override
    public Optional<ID> getVotedFor() {
        return Optional.ofNullable(votedFor);
    }
}
