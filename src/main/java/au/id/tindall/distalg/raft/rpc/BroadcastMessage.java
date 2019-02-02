package au.id.tindall.distalg.raft.rpc;

import java.io.Serializable;

public class BroadcastMessage<ID extends Serializable> extends RpcMessage<ID> {

    public BroadcastMessage(ID source) {
        super(source);
    }
}
