package au.id.tindall.distalg.raft.rpc;

import java.io.Serializable;

public class UnicastMessage<ID extends Serializable> extends RpcMessage<ID> {

    private final ID destination;

    public UnicastMessage(ID source, ID destination) {
        super(source);
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
