package au.id.tindall.distalg.raft.comms;

import au.id.tindall.distalg.raft.rpc.server.RpcMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class QueuedSendingStrategy implements SendingStrategy {

    private List<RpcMessage<Long>> messageQueue;
    private Consumer<RpcMessage<Long>> dispatchFunction;

    public QueuedSendingStrategy() {
        messageQueue = new ArrayList<>();
    }

    @Override
    public void setDispatchFunction(Consumer<RpcMessage<Long>> dispatchFunction) {
        this.dispatchFunction = dispatchFunction;
    }

    @Override
    public void send(RpcMessage<Long> message) {
        messageQueue.add(message);
    }

    public void flush() {
        List<RpcMessage<Long>> oldMessages = messageQueue;
        messageQueue = new ArrayList<>();
        oldMessages.forEach(this.dispatchFunction);
    }

    public void fullyFlush() {
        while (!messageQueue.isEmpty()) {
            flush();
        }
    }
}
