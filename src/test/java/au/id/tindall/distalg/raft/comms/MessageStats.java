package au.id.tindall.distalg.raft.comms;

import au.id.tindall.distalg.raft.rpc.server.RpcMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.TreeMap;

import static java.lang.String.format;

public class MessageStats {

    private static final Logger LOGGER = LogManager.getLogger();

    private final Map<String, Integer> messageCounts = new TreeMap<>();

    public void recordMessageSent(RpcMessage<Long> rpcMessage) {
        messageCounts.put(rpcMessage.getClass().getSimpleName(), messageCounts.getOrDefault(rpcMessage.getClass().getSimpleName(), 0) + 1);
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
        LOGGER.warn(stringBuilder.toString());
    }
}
