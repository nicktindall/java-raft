package au.id.tindall.distalg.raft.comms.simulated;

import au.id.tindall.distalg.raft.rpc.server.RpcMessage;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Route all messages immediately from source to destination
 *
 * @param <I>
 */
class InstantRouter<I> implements Router<I> {

    @Override
    public boolean route(Map<I, QueueingCluster<I>> clusters) {
        AtomicBoolean routedMessages = new AtomicBoolean(false);
        clusters.values().stream()
                .flatMap(qc -> qc.queues().entrySet().stream())
                .forEach(qi -> {
                    QueueingCluster<I> destination = clusters.get(qi.getKey());
                    Queue<RpcMessage<I>> queue = qi.getValue();
                    RpcMessage<I> message;
                    while ((message = queue.poll()) != null) {
                        destination.deliver(message);
                        routedMessages.set(true);
                    }
                });
        return routedMessages.get();
    }
}
