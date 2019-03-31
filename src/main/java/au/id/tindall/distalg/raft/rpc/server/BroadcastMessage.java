package au.id.tindall.distalg.raft.rpc.server;

import java.io.Serializable;

import au.id.tindall.distalg.raft.log.Term;

public class BroadcastMessage<ID extends Serializable> extends RpcMessage<ID> {

    public BroadcastMessage(Term term, ID source) {
        super(term, source);
    }
}
