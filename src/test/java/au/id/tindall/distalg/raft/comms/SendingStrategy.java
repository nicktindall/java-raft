package au.id.tindall.distalg.raft.comms;

import au.id.tindall.distalg.raft.rpc.server.RpcMessage;

public interface SendingStrategy {

    void send(Long destination, RpcMessage<Long> message);

    RpcMessage<Long> poll(Long serverId);
}
