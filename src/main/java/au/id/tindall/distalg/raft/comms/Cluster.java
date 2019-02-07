package au.id.tindall.distalg.raft.comms;

import java.io.Serializable;
import java.util.Set;

import au.id.tindall.distalg.raft.rpc.RpcMessage;

public interface Cluster<ID extends Serializable> {

    void send(RpcMessage<ID> message);

    boolean isQuorum(Set<ID> receivedVotes);

    Set<ID> getMemberIds();
}
