package au.id.tindall.distalg.raft.comms.simulated;

import au.id.tindall.distalg.raft.rpc.server.RpcMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Comparator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Random;
import java.util.Stack;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;

class DelayingReorderingRouter<I> implements Router<I> {

    private static final Logger LOGGER = LogManager.getLogger();

    private static final Comparator<MessageSlot<?>> ARRIVAL_TIME_ASCENDING = Comparator.comparingLong(MessageSlot::getArrivalTimeNanos);
    private final LongSupplier nanoTimeSupplier;
    private final float packetDropProbability;
    private final long minimumDelayNanos;
    private final long maximumDelayNanos;
    private final Random random = new Random();
    private final Queue<MessageSlot<I>> delayedMessages = new PriorityQueue<>(ARRIVAL_TIME_ASCENDING);
    private final Stack<MessageSlot<I>> slotPool = new Stack<>();

    public DelayingReorderingRouter(float packetDropProbability, long minimumDelay, long maximumDelay, TimeUnit delayUnit) {
        this(System::nanoTime, packetDropProbability, minimumDelay, maximumDelay, delayUnit);
    }

    public DelayingReorderingRouter(LongSupplier nanoTimeSupplier, float packetDropProbability, long minimumDelay, long maximumDelay, TimeUnit delayUnit) {
        this.nanoTimeSupplier = nanoTimeSupplier;
        this.packetDropProbability = packetDropProbability;
        this.minimumDelayNanos = delayUnit.toNanos(minimumDelay);
        this.maximumDelayNanos = delayUnit.toNanos(maximumDelay);
    }

    @Override
    public boolean route(Map<I, QueueingCluster<I>> clusters) {
        AtomicBoolean didSomething = new AtomicBoolean();
        queueNewMessages(clusters, didSomething);
        deliverQueuedMessages(clusters, didSomething);
        return didSomething.get();
    }

    private void deliverQueuedMessages(Map<I, QueueingCluster<I>> inboxes, AtomicBoolean didSomething) {
        MessageSlot<I> nextMessage;
        long lastMessageArrivalTime = -1;
        while (true) {
            nextMessage = delayedMessages.peek();
            if (nextMessage != null && nextMessage.getArrivalTimeNanos() < nanoTimeSupplier.getAsLong()) {
                delayedMessages.poll();
                assert lastMessageArrivalTime == -1 || nextMessage.getArrivalTimeNanos() >= lastMessageArrivalTime : "Messages not being delivered in arrival time order";
                lastMessageArrivalTime = nextMessage.getArrivalTimeNanos();
                QueueingCluster<I> inbox = inboxes.get(nextMessage.getDestination());
                if (inbox != null) {
                    inbox.deliver(nextMessage.getMessage());
                } else {
                    LOGGER.warn("No inbox found for destination {}", nextMessage.getDestination());
                }
                slotPool.push(nextMessage);
                didSomething.set(true);
            } else {
                break;
            }
        }
    }

    private void queueNewMessages(Map<I, QueueingCluster<I>> clusters, AtomicBoolean didSomething) {
        clusters.values().stream()
                .flatMap(qc -> qc.queues().entrySet().stream())
                .forEach(qi -> {
                    I destination = qi.getKey();
                    Queue<RpcMessage<I>> queue = qi.getValue();
                    RpcMessage<I> message;
                    while ((message = queue.poll()) != null) {
                        didSomething.set(true);
                        if (random.nextFloat() < packetDropProbability) {
                            continue;
                        }
                        MessageSlot<I> messageSlot = slotPool.isEmpty() ? new MessageSlot<>() : slotPool.pop();
                        messageSlot.populate(destination, randomArrivalTime(), message);
                        delayedMessages.offer(messageSlot);
                    }
                });
    }

    private long randomArrivalTime() {
        if (minimumDelayNanos == maximumDelayNanos) {
            return nanoTimeSupplier.getAsLong();
        }
        return nanoTimeSupplier.getAsLong() + random.nextLong(minimumDelayNanos, maximumDelayNanos);
    }

    private static final class MessageSlot<I> {
        private I destination;
        private long arrivalTimeNanos;
        private RpcMessage<I> message;

        public void populate(I destination, long arrivalTimeNanos, RpcMessage<I> message) {
            this.destination = destination;
            this.arrivalTimeNanos = arrivalTimeNanos;
            this.message = message;
        }

        public I getDestination() {
            return destination;
        }

        public long getArrivalTimeNanos() {
            return arrivalTimeNanos;
        }

        public RpcMessage<I> getMessage() {
            return message;
        }
    }
}
