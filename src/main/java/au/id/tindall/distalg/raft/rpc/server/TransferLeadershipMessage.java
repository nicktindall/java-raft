package au.id.tindall.distalg.raft.rpc.server;

import au.id.tindall.distalg.raft.log.Term;

import java.io.Serializable;

public class TransferLeadershipMessage<I extends Serializable> extends RpcMessage<I> {

    public TransferLeadershipMessage(Term term, I serverId) {
        super(term, serverId);
    }
}
