package au.id.tindall.distalg.raft.rpc;

import java.io.Serializable;

import au.id.tindall.distalg.raft.log.Term;

public class UnicastMessage<ID extends Serializable> extends RpcMessage<ID> {

    private final ID destination;

    public UnicastMessage(Term term, ID source, ID destination) {
        super(term, source);
        this.destination = destination;
    }

    public ID getDestination() {
        return destination;
    }

    @Override
    public String toString() {
        return "UnicastMessage{" +
                "destination=" + destination +
                "} " + super.toString();
    }
}
