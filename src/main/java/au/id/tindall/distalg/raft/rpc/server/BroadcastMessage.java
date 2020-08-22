package au.id.tindall.distalg.raft.rpc.server;

import au.id.tindall.distalg.raft.log.Term;

import java.io.Serializable;

public class BroadcastMessage<ID extends Serializable> extends RpcMessage<ID> {

    public BroadcastMessage(Term term, ID source) {
        super(term, source);
    }
}
