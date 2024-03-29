package au.id.tindall.distalg.raft.comms;

import au.id.tindall.distalg.raft.rpc.server.RpcMessage;
import org.apache.logging.log4j.Logger;

import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;

import static java.lang.String.format;
import static org.apache.logging.log4j.LogManager.getLogger;

public class DelayedMultipathSendingStrategy implements SendingStrategy {

    private static final Logger LOGGER = getLogger();

    private final float percentDropped;
    private final ConcurrentHashMap<Long, ServerMessageQueue> serverPaths;
    private final long minLatencyMicros;
    private final long maxLatencyMicros;

    public DelayedMultipathSendingStrategy(float percentDropped, long minLatencyMicros, long maxLatencyMicros) {
        if (percentDropped < 0.0 || percentDropped > 1.0) {
            throw new IllegalArgumentException("Percent delayed must be between 0.0 and 1.0");
        }
        if (minLatencyMicros < 0 || maxLatencyMicros < 0 || maxLatencyMicros < minLatencyMicros) {
            throw new IllegalArgumentException(format("Invalid values for min/max latency: minLatencyMicros=%,d, maxLatencyMicros=%,d", minLatencyMicros, maxLatencyMicros));
        }
        this.percentDropped = percentDropped;
        this.minLatencyMicros = minLatencyMicros;
        this.maxLatencyMicros = maxLatencyMicros;
        serverPaths = new ConcurrentHashMap<>();
    }

    @Override
    public void send(Long destination, RpcMessage<Long> message) {
        final ServerMessageQueue serverMessageQueue = serverPaths.get(destination);
        if (serverMessageQueue != null) {
            serverMessageQueue.send(message);
        }
    }

    @Override
    public RpcMessage<Long> poll(Long serverId) {
        final ServerMessageQueue serverMessageQueue = serverPaths.get(serverId);
        if (serverMessageQueue != null) {
            return serverMessageQueue.poll();
        }
        return null;
    }

    @Override
    public void onStop(Long serverId) {
        serverPaths.remove(serverId);
    }

    @Override
    public void onStart(Long serverId) {
        serverPaths.put(serverId, new ServerMessageQueue(percentDropped, minLatencyMicros, maxLatencyMicros));
    }

    public void expire() {
        int beforeSize = serverPaths.size();
        boolean removed = serverPaths.values().removeIf(ServerMessageQueue::isStale);
        if (removed) {
            LOGGER.debug("Expired {} server paths", beforeSize - serverPaths.size());
        }
    }

    public void clear() {
        serverPaths.values().forEach(ServerMessageQueue::logStats);
        serverPaths.clear();
    }

    private static class ServerMessageQueue {

        private static final int INACTIVITY_BEFORE_STALE_MS = 10_000;
        private static final int MESSAGE_DELAY_WARNING_MS = 50;
        private final PriorityBlockingQueue<MessageSlot> messageQueue;
        private final MessageSlotPool messageSlotPool;
        private final float percentDropped;
        private final long minLatencyNanos;
        private final long maxLatencyNanos;
        private volatile long lastSend;
        private volatile long lastPoll;
        private volatile long lastArriveTimeNanos = Long.MIN_VALUE;

        public ServerMessageQueue(float percentDropped, long minLatencyMicros, long maxLatencyMicros) {
            this.percentDropped = percentDropped;
            this.minLatencyNanos = minLatencyMicros * 1_000;
            this.maxLatencyNanos = maxLatencyMicros * 1_000;
            lastSend = System.currentTimeMillis();
            lastPoll = System.currentTimeMillis();
            messageQueue = new PriorityBlockingQueue<>();
            messageSlotPool = new MessageSlotPool(500);
        }

        public RpcMessage<Long> poll() {
            lastPoll = System.currentTimeMillis();
            final MessageSlot peek = messageQueue.peek();
            final long nowNanos = System.nanoTime();
            if (peek != null && peek.arrivalTimeNanos < nowNanos) {
                assertArrivalTimeIncreasing(peek);
                checkMessageDelay(peek.arrivalTimeNanos, nowNanos);
                final MessageSlot nextMessage = messageQueue.poll();
                RpcMessage<Long> message = nextMessage.message;
                messageSlotPool.ret(nextMessage);
                return message;
            }
            return null;
        }

        private static void checkMessageDelay(long scheduledArrivalTimeNanos, long nowNanos) {
            final long messageDelayedMillis = (nowNanos - scheduledArrivalTimeNanos) / 1_000_000;
            if (messageDelayedMillis > MESSAGE_DELAY_WARNING_MS) {
                LOGGER.warn("Message delayed by " + messageDelayedMillis + "ms");
            }
        }

        private void assertArrivalTimeIncreasing(MessageSlot peek) {
            if (lastArriveTimeNanos > peek.arrivalTimeNanos) {
                LOGGER.warn(format("lastArriveTime (%,d) > arrivalTimeNanos (%,d)", lastArriveTimeNanos, peek.arrivalTimeNanos));
            }
            lastArriveTimeNanos = peek.arrivalTimeNanos;
        }

        public void send(RpcMessage<Long> rpcMessage) {
            lastSend = System.currentTimeMillis();
            if (ThreadLocalRandom.current().nextFloat() < percentDropped) {
                // Drop it
            } else {
                long latency = (minLatencyNanos == maxLatencyNanos) ? minLatencyNanos : ThreadLocalRandom.current().nextLong(minLatencyNanos, maxLatencyNanos);
                messageQueue.add(messageSlotPool.get(rpcMessage, latency));
            }
        }

        public boolean isStale() {
            final long now = System.currentTimeMillis();
            return now - lastSend > INACTIVITY_BEFORE_STALE_MS && now - lastPoll > INACTIVITY_BEFORE_STALE_MS;
        }

        public void logStats() {
            LOGGER.debug("Pool high-water-mark was {}", messageSlotPool.highWaterMark);
        }
    }

    private static class MessageSlotPool {

        private ArrayBlockingQueue<MessageSlot> pool;
        private int highWaterMark = 0;

        public MessageSlotPool(int capacity) {
            this.pool = new ArrayBlockingQueue<>(capacity);
        }

        public MessageSlot get(RpcMessage<Long> message, long delayNanos) {
            final MessageSlot nextFromPool = pool.poll();
            return Objects.requireNonNullElseGet(nextFromPool, MessageSlot::new)
                    .reset(message, delayNanos);
        }

        public void ret(MessageSlot messageSlot) {
            if (pool.offer(messageSlot.clear())) {
                final int size = pool.size();
                if (size > highWaterMark) {
                    highWaterMark = size;
                }
            } else {
                // pool is full, discard
                LOGGER.warn("MessageSlot pool was full, discarding one");
            }
        }
    }

    private static class MessageSlot implements Comparable<MessageSlot> {
        private volatile long arrivalTimeNanos;
        private RpcMessage<Long> message;

        public MessageSlot reset(RpcMessage<Long> message, long delayNanos) {
            arrivalTimeNanos = System.nanoTime() + delayNanos;
            this.message = message;
            return this;
        }

        public MessageSlot clear() {
            this.message = null;
            return this;
        }

        @Override
        public int compareTo(MessageSlot o) {
            return (int) (this.arrivalTimeNanos - o.arrivalTimeNanos);
        }
    }
}
