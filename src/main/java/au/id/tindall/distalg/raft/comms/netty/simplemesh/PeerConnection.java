package au.id.tindall.distalg.raft.comms.netty.simplemesh;

import au.id.tindall.distalg.raft.comms.netty.simplemesh.messages.BlockMessage;
import au.id.tindall.distalg.raft.comms.netty.simplemesh.messages.PeerDetails;
import au.id.tindall.distalg.raft.serialisation.Streamable;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static au.id.tindall.distalg.raft.comms.netty.simplemesh.ConnectionManager.CONNECTION_SOURCE;
import static au.id.tindall.distalg.raft.comms.netty.simplemesh.ConnectionManager.CONNECTION_TYPE;
import static au.id.tindall.distalg.raft.comms.netty.simplemesh.ConnectionManager.DELAY_GROWTH_RATE;
import static au.id.tindall.distalg.raft.comms.netty.simplemesh.ConnectionManager.INITIAL_DELAY_MS;
import static au.id.tindall.distalg.raft.comms.netty.simplemesh.ConnectionManager.MAX_DELAY_MS;
import static au.id.tindall.distalg.raft.comms.netty.simplemesh.ConnectionManager.MESSAGE_RECEIVER;

/**
 * We try to maintain a single inbound and a single outbound connection to each peer
 */
public class PeerConnection implements ConnectionManager.ConnectionSource, Closeable {

    private static final Logger LOGGER = LogManager.getLogger();
    private static final int MAX_CONNECTION_ATTEMPTS = 10;
    private static final int MESSAGE_BUFFER_SIZE = 10;

    private final int remoteId;
    private final Queue<Message> inboundMessages = new ConcurrentLinkedQueue<>();
    private PeerDetails currentPeerDetails;
    private volatile Connection outboundConnection;
    private volatile Connection inboundConnection;
    private boolean outboundConnectionInProgress;
    private long nextDelayMs;
    private long nextConnectAttemptMs = 0L;
    private boolean disconnected = false;
    private final AtomicInteger failedConnectionAttempts = new AtomicInteger();
    private final AtomicBoolean readingFromChannels = new AtomicBoolean(true);

    public PeerConnection(int remoteId) {
        this.remoteId = remoteId;
    }

    public void reset() {
        clearInboundMessages();
    }

    public synchronized boolean tryHandshake(Channel channel, PeerDetails peerDetails) {
        if (disconnected) {
            channel.disconnect();
            return false;
        }
        final DetailsUpdateResult detailsUpdateResult = updatePeerDetails(peerDetails);
        if (detailsUpdateResult == DetailsUpdateResult.OLD_NODE) {
            LOGGER.warn("Received connection from stale node, rejecting. remoteId={}", remoteId);
            blockAndDisconnect(channel);
            return false;
        } else {
            channel.attr(MESSAGE_RECEIVER).set(this::queueInbound);
            switch (channel.attr(CONNECTION_TYPE).get()) {
                case INBOUND -> inboundConnection = updateConnection(inboundConnection, channel, peerDetails);
                case OUTBOUND -> outboundConnection = updateConnection(outboundConnection, channel, peerDetails);
                default -> throw new IllegalStateException("Unexpected value: " + channel.attr(CONNECTION_TYPE).get());
            }
        }
        return true;
    }

    public synchronized DetailsUpdateResult updatePeerDetails(PeerDetails peerDetails) {
        if (currentPeerDetails != null) {
            if (peerDetails.nodeTimestamp() < currentPeerDetails.nodeTimestamp()) {
                // We have a newer peer details, ignore it
                return DetailsUpdateResult.OLD_NODE;
            } else if (peerDetails.nodeTimestamp() > currentPeerDetails.nodeTimestamp()) {
                LOGGER.info("Connected to a new node, blocking old node: {} (hasOutboundConnection={}, hasInboundConnection={}, new={}, old={})",
                        peerDetails.id(), outboundConnection == null, inboundConnection == null, peerDetails, currentPeerDetails);
                blockAndDisconnect(outboundConnection);
                blockAndDisconnect(inboundConnection);
                inboundConnection = null;
                outboundConnection = null;
                currentPeerDetails = peerDetails;
                return DetailsUpdateResult.NEW_NODE;
            } else {
                if (peerDetails.detailsTimestamp() > currentPeerDetails.detailsTimestamp()) {
                    currentPeerDetails = peerDetails;
                    LOGGER.debug("Got new details (new={}, old={})", peerDetails, currentPeerDetails);
                    return DetailsUpdateResult.NEW_DETAILS;
                } else {
                    return DetailsUpdateResult.NO_CHANGE;
                }
            }
        } else {
            currentPeerDetails = peerDetails;
            LOGGER.info("Got first details {}", peerDetails);
            return DetailsUpdateResult.INITIAL;
        }
    }

    public PeerDetails getPeerDetails() {
        return currentPeerDetails;
    }

    public boolean send(Streamable message) {
        final Connection firstConnection, secondConnection;
        if (ThreadLocalRandom.current().nextBoolean()) {
            firstConnection = outboundConnection;
            secondConnection = inboundConnection;
        } else {
            firstConnection = inboundConnection;
            secondConnection = outboundConnection;
        }
        if (firstConnection != null && firstConnection.channel.isOpen()) {
            firstConnection.channel.writeAndFlush(message);
            return true;
        } else if (secondConnection != null && secondConnection.channel.isOpen()) {
            secondConnection.channel.writeAndFlush(message);
            return true;
        }
        return false;
    }

    public boolean isConnected() {
        return (outboundConnection != null && outboundConnection.channel.isOpen())
                || (inboundConnection != null && inboundConnection.channel.isOpen());
    }

    @Override
    public String toString() {
        return "PeerConnections{" +
                "remoteId=" + remoteId +
                ", outboundConnection=" + outboundConnection +
                ", inboundConnection=" + inboundConnection +
                '}';
    }

    @Override
    public synchronized Iterator<String> getNextConnectAttempt() {
        if (!disconnected
                && (outboundConnection == null || !outboundConnection.channel().isOpen())
                && !outboundConnectionInProgress
                && nextConnectAttemptMs < System.currentTimeMillis()
                && currentPeerDetails != null) {
            outboundConnectionInProgress = true;
            return currentPeerDetails.socketAddresses().iterator();
        }
        return null;
    }

    @Override
    public synchronized void onSuccess() {
        nextConnectAttemptMs = 0L;
        nextDelayMs = INITIAL_DELAY_MS;
        outboundConnectionInProgress = false;
        failedConnectionAttempts.set(0);
    }

    @Override
    public synchronized void onFailed(boolean retryable) {
        outboundConnectionInProgress = false;
        if (retryable && failedConnectionAttempts.getAndIncrement() < MAX_CONNECTION_ATTEMPTS) {
            nextConnectAttemptMs = System.currentTimeMillis() + nextDelayMs;
            nextDelayMs = Math.min((long) (nextDelayMs * DELAY_GROWTH_RATE), MAX_DELAY_MS);
        } else {
            if (!retryable) {
                LOGGER.info("Encountered non-retryable connection failure to {}, Giving up.", remoteId);
            } else {
                LOGGER.info("Haven't connected to {} in {} attempts. Giving up.", remoteId, MAX_CONNECTION_ATTEMPTS);
            }
            nextConnectAttemptMs = 0L;
            disconnected = true;
        }
    }

    private void queueInbound(Message message) {
        inboundMessages.offer(message);
        if (inboundMessages.size() >= MESSAGE_BUFFER_SIZE) {
            readingFromChannels.set(false);
        }
    }

    public Message poll() {
        final Message poll = inboundMessages.poll();
        if (inboundMessages.size() < MESSAGE_BUFFER_SIZE &&
                readingFromChannels.compareAndSet(false, true)) {
            resumeReading();
        }
        return poll;
    }

    private void resumeReading() {
        if (inboundConnection != null) {
            inboundConnection.channel.read();
        }
        if (outboundConnection != null) {
            outboundConnection.channel.read();
        }
    }

    public void connect() {
        this.disconnected = false;
    }

    public void disconnect() {
        this.disconnected = true;
        if (inboundConnection != null) {
            inboundConnection.channel().close();
        }
        if (outboundConnection != null) {
            outboundConnection.channel().close();
        }
        inboundConnection = null;
        outboundConnection = null;
        clearInboundMessages();
    }

    private void clearInboundMessages() {
        inboundMessages.clear();
    }

    @Override
    public void close() {
        disconnect();
    }

    public void onDisconnect(ChannelHandlerContext ctx) {
        final Connection ic = inboundConnection;
        if (ic != null && ctx.channel() == ic.channel()) {
            ic.channel().close();
            inboundConnection = null;
        }
        final Connection oc = outboundConnection;
        if (oc != null && ctx.channel() == oc.channel()) {
            oc.channel().close();
            outboundConnection = null;
        }
    }

    public boolean shouldRead() {
        return readingFromChannels.get();
    }

    public enum DetailsUpdateResult {
        OLD_NODE(false),
        NO_CHANGE(false),
        NEW_NODE(true),
        NEW_DETAILS(true),
        INITIAL(true);

        private final boolean isNew;

        DetailsUpdateResult(boolean isNew) {
            this.isNew = isNew;
        }

        public boolean isNew() {
            return isNew;
        }
    }

    /**
     * If we end up with a duplicate connection, we deterministically keep the one with the highest initiator port
     * <p>
     * Disconnect one channel and return the one to keep
     */
    private Connection updateConnection(Connection existingConnection, Channel newChannel, PeerDetails peerDetails) {
        if (existingConnection != null) {
            if (!existingConnection.channel.isOpen()) {
                return new Connection(peerDetails, newChannel);
            } else {
                ConnectionManager.ConnectionSource connectionSource = newChannel.attr(CONNECTION_SOURCE).get();
                LOGGER.debug("Duplicate connection established, closing one [source={}]", connectionSource);
                if (getInitiatorPort(existingConnection.channel) < getInitiatorPort(newChannel)) {
                    existingConnection.channel.disconnect();
                    return new Connection(peerDetails, newChannel);
                } else {
                    newChannel.disconnect();
                    return existingConnection;
                }
            }
        } else {
            return new Connection(peerDetails, newChannel);
        }
    }

    private int getInitiatorPort(Channel channel) {
        final ConnectionManager.ConnectionType connectionType = channel.attr(CONNECTION_TYPE).get();
        return switch (connectionType) {
            case INBOUND -> ((InetSocketAddress) channel.remoteAddress()).getPort();
            case OUTBOUND -> ((InetSocketAddress) channel.localAddress()).getPort();
        };
    }

    private void blockAndDisconnect(Channel channel) {
        channel.writeAndFlush(BlockMessage.INSTANCE);
        channel.disconnect();
    }

    private void blockAndDisconnect(Connection connection) {
        if (connection != null) {
            blockAndDisconnect(connection.channel);
        }
    }

    private record Connection(PeerDetails peerDetails, Channel channel) {
    }
}
