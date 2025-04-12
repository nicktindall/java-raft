package au.id.tindall.distalg.raft.rpc.client;

public interface ClientResponseMessage<I> {

    boolean isFromLeader();

    default I getLeaderHint() {
        return null;
    }
}
