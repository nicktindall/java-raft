package au.id.tindall.distalg.raft.rpc.server;

import au.id.tindall.distalg.raft.log.Term;

import java.io.Serializable;

public class TransferLeadershipMessage<ID extends Serializable> extends RpcMessage<ID> {

    public TransferLeadershipMessage(Term term, ID serverId) {
        super(term, serverId);
    }
}
