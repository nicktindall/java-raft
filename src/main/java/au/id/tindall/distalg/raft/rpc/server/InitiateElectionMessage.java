package au.id.tindall.distalg.raft.rpc.server;

import au.id.tindall.distalg.raft.log.Term;

import java.io.Serializable;

public class InitiateElectionMessage<ID extends Serializable> extends RpcMessage<ID> {

    public InitiateElectionMessage(Term term, ID source) {
        super(term, source);
    }
}
