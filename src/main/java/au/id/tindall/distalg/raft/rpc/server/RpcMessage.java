package au.id.tindall.distalg.raft.rpc.server;

import au.id.tindall.distalg.raft.log.Term;

import java.io.Serializable;

public abstract class RpcMessage<I extends Serializable> implements Serializable {

    private final Term term;
    private final I source;

    protected RpcMessage(Term term, I source) {
        this.term = term;
        this.source = source;
    }

    public Term getTerm() {
        return term;
    }

    public I getSource() {
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
