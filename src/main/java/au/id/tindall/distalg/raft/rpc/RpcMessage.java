package au.id.tindall.distalg.raft.rpc;

import java.io.Serializable;

public abstract class RpcMessage<ID extends Serializable> implements Serializable {

    private final ID source;

    public RpcMessage(ID source) {
        this.source = source;
    }

    public ID getSource() {
        return source;
    }

    @Override
    public String toString() {
        return "RpcMessage{" +
                "source=" + source +
                '}';
    }
}
