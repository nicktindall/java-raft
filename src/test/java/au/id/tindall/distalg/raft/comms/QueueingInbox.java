package au.id.tindall.distalg.raft.comms;

import au.id.tindall.distalg.raft.rpc.server.RpcMessage;
import au.id.tindall.distalg.raft.util.Closeables;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class QueueingInbox implements Inbox<Long>, Closeable {

    private final long localHostId;
    private final SendingStrategy sendingStrategy;
    private final List<ClientConnection<Long>> clientConnections;

    public QueueingInbox(long localHostId, SendingStrategy sendingStrategy) {
        this.localHostId = localHostId;
        this.sendingStrategy = sendingStrategy;
        clientConnections = new CopyOnWriteArrayList<>();
    }

    public void send(RpcMessage<Long> rpcMessage) {
        sendingStrategy.send(localHostId, rpcMessage);
    }

    @Override
    public boolean processNextMessage(MessageProcessor<Long> messageProcessor) {
        final RpcMessage<Long> rpcMessage = sendingStrategy.poll(localHostId);
        if (rpcMessage != null) {
            messageProcessor.handle(rpcMessage);
            return true;
        }
        for (final ClientConnection<Long> conn : clientConnections) {
            if (conn.processAMessage(messageProcessor)) {
                return true;
            }
        }
        return false;
    }

    public ClientConnection<Long> openConnection() {
        final ClientConnection<Long> conn = new ClientConnection<>();
        clientConnections.add(conn);
        return conn;
    }

    public void reset() {
        sendingStrategy.onStart(localHostId);
        clientConnections.clear();
    }

    @Override
    public void close() throws IOException {
        Closeables.closeQuietly(clientConnections);
    }
}
