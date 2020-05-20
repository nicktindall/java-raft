package au.id.tindall.distalg.raft.comms;

import au.id.tindall.distalg.raft.rpc.server.RpcMessage;

import java.util.function.Consumer;

public interface SendingStrategy {

    void setDispatchFunction(Consumer<RpcMessage<Long>> dispatchFunction);

    void send(RpcMessage<Long> message);
}
