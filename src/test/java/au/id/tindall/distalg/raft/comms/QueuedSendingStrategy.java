package au.id.tindall.distalg.raft.comms;

import au.id.tindall.distalg.raft.Server;
import au.id.tindall.distalg.raft.rpc.server.RpcMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class QueuedSendingStrategy implements SendingStrategy {

    private List<DestinationAndMessage> messageQueue;

    public QueuedSendingStrategy() {
        messageQueue = new ArrayList<>();
    }

    @Override
    public void send(Long destinationId, RpcMessage<Long> message) {
        messageQueue.add(new DestinationAndMessage(destinationId, message));
    }

    @Override
    public RpcMessage<Long> poll(Long serverId) {
        throw new UnsupportedOperationException("Use flush/fullyFlush");
    }

    @Override
    public void onStop(Long localId) {
    }

    @Override
    public void onStart(Long localId) {
    }

    public void flush(Map<Long, Server<Long>> serverMap) {
        List<DestinationAndMessage> oldMessages = messageQueue;
        messageQueue = new ArrayList<>();
        oldMessages.forEach(msg -> serverMap.get(msg.destinationId).handle(msg.message));
    }

    public void fullyFlush(Map<Long, Server<Long>> serverMap) {
        while (!messageQueue.isEmpty()) {
            flush(serverMap);
        }
    }

    private static class DestinationAndMessage {
        private final Long destinationId;
        private final RpcMessage<Long> message;

        public DestinationAndMessage(Long destinationId, RpcMessage<Long> message) {
            this.destinationId = destinationId;
            this.message = message;
        }
    }
}
