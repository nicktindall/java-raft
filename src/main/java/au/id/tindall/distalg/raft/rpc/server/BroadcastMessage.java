package au.id.tindall.distalg.raft.rpc.server;

import au.id.tindall.distalg.raft.log.Term;

import java.io.Serializable;

public class BroadcastMessage<I extends Serializable> extends RpcMessage<I> {

    public BroadcastMessage(Term term, I source) {
        super(term, source);
    }
}
