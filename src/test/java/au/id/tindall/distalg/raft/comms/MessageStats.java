package au.id.tindall.distalg.raft.comms;

import au.id.tindall.distalg.raft.rpc.server.AppendEntriesRequest;
import au.id.tindall.distalg.raft.rpc.server.RpcMessage;
import au.id.tindall.distalg.raft.serialisation.ByteBufferIO;
import au.id.tindall.distalg.raft.serialisation.LongIDSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

import static java.lang.String.format;

/**
 * This is all very crude
 */
public class MessageStats {

    private static final Logger LOGGER = LogManager.getLogger();

    private final Map<String, Integer> messageCounts = new ConcurrentSkipListMap<>();
    private final ThreadLocal<ByteBufferIO> bbioTL = ThreadLocal.withInitial(() -> ByteBufferIO.elastic(LongIDSerializer.INSTANCE));
    private long totalMessageBytes = 0;
    private long managementOverheadMessageBytes = 0;

    public void recordMessageSent(RpcMessage<Long> rpcMessage) {
        messageCounts.put(rpcMessage.getClass().getSimpleName(), messageCounts.getOrDefault(rpcMessage.getClass().getSimpleName(), 0) + 1);
        long messageSizeInBytes = messageSizeInBytes(rpcMessage);
        if (isManagementOverhead(rpcMessage)) {
            this.managementOverheadMessageBytes += messageSizeInBytes;
        }
        this.totalMessageBytes += messageSizeInBytes;
    }

    public void logStats() {
        StringBuilder stringBuilder = new StringBuilder();
        int totalMessages = 0;
        stringBuilder.append("\n\n");
        for (Map.Entry<String, Integer> count : messageCounts.entrySet()) {
            stringBuilder.append(format("%-35s %,10d\n", count.getKey(), count.getValue()));
            totalMessages += count.getValue();
        }
        stringBuilder.append(format("----------------------------------------------\n"));
        stringBuilder.append(format("%-35s %,10d\n", "Total", totalMessages));
        stringBuilder.append(format("%-35s %9d%%\n", "Management Overhead", Math.round(((double) managementOverheadMessageBytes / totalMessageBytes) * 100)));
        LOGGER.info(stringBuilder.toString());
    }

    private long messageSizeInBytes(RpcMessage<Long> rpcMessage) {
        final ByteBufferIO byteBufferIO = bbioTL.get();
        byteBufferIO.setWritePosition(0);
        byteBufferIO.writeStreamable(rpcMessage);
        return byteBufferIO.getWritePosition();
    }

    /**
     * Anything that isn't sending log entries we consider management overhead
     */
    private boolean isManagementOverhead(RpcMessage<Long> message) {
        return !(message instanceof AppendEntriesRequest && ((AppendEntriesRequest<Long>) message).getEntries().size() > 0);
    }
}
