package au.id.tindall.distalg.raft.rpc.server;

import au.id.tindall.distalg.raft.log.Term;

import java.io.Serializable;

public abstract class RpcMessage<ID extends Serializable> implements Serializable {

    private final Term term;
    private final ID source;

    public RpcMessage(Term term, ID source) {
        this.term = term;
        this.source = source;
    }

    public Term getTerm() {
        return term;
    }

    public ID getSource() {
        return source;
    }

    @Override
    public String toString() {
        return "RpcMessage{" +
                "term=" + term +
                ", source=" + source +
                '}';
    }
}
