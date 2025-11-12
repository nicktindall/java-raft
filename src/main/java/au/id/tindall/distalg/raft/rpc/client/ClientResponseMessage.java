package au.id.tindall.distalg.raft.rpc.client;

import au.id.tindall.distalg.raft.serialisation.Streamable;

public interface ClientResponseMessage<I> extends Streamable {

    boolean isFromLeader();

    default I getLeaderHint() {
        return null;
    }
}
