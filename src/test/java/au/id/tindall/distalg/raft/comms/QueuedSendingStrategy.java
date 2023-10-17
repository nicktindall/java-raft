package au.id.tindall.distalg.raft.comms;

import au.id.tindall.distalg.raft.rpc.server.RpcMessage;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

public class QueuedSendingStrategy implements SendingStrategy {

    private final Map<Long, Queue<RpcMessage<Long>>> queues;

    public QueuedSendingStrategy() {
        queues = new HashMap<>();
    }

    @Override
    public void send(Long destinationId, RpcMessage<Long> message) {
        final Queue<RpcMessage<Long>> serverQueue = queues.get(destinationId);
        if (serverQueue != null) {
            serverQueue.offer(message);
        }
    }

    @Override
    public RpcMessage<Long> poll(Long serverId) {
        final Queue<RpcMessage<Long>> serverQueue = queues.get(serverId);
        if (serverQueue != null) {
            return serverQueue.poll();
        }
        return null;
    }

    @Override
    public void onStop(Long localId) {
        queues.remove(localId);
    }

    @Override
    public void onStart(Long localId) {
        queues.put(localId, new LinkedList<>());
    }
}
