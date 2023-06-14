package au.id.tindall.distalg.raft.rpc.server;

import au.id.tindall.distalg.raft.log.Term;

import java.io.Serializable;

public class UnicastMessage<I extends Serializable> extends RpcMessage<I> {

    private final I destination;

    public UnicastMessage(Term term, I source, I destination) {
        super(term, source);
        this.destination = destination;
    }

    public I getDestination() {
        return destination;
    }

    @Override
    public String toString() {
        return "UnicastMessage{" +
                "destination=" + destination +
                "} " + super.toString();
    }
}
