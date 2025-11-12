package au.id.tindall.distalg.raft.comms.netty.simplemesh;

import au.id.tindall.distalg.raft.comms.netty.simplemesh.messages.BlockMessage;
import au.id.tindall.distalg.raft.comms.netty.simplemesh.messages.GossipMessage;
import au.id.tindall.distalg.raft.comms.netty.simplemesh.messages.PeerDetails;
import au.id.tindall.distalg.raft.serialisation.Streamable;
import au.id.tindall.distalg.raft.util.Closeables;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.Future;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class ConnectionManager extends ChannelInitializer<Channel> implements Closeable {

    private static final Logger LOGGER = LogManager.getLogger();
    static final long INITIAL_DELAY_MS = 500;
    static final long MAX_DELAY_MS = 10_000;
    static final double DELAY_GROWTH_RATE = 1.5;
    private final AtomicInteger roundRobinCounter = new AtomicInteger(0);

    public enum ConnectionType {
        INBOUND,
        OUTBOUND
    }

    public static final AttributeKey<ConnectionManager> CONNECTION_MANAGER = AttributeKey.newInstance("ConnectionManager");
    public static final AttributeKey<Consumer<Message>> MESSAGE_RECEIVER = AttributeKey.newInstance("MessageReceiver");
    public static final AttributeKey<ConnectionType> CONNECTION_TYPE = AttributeKey.newInstance("ConnectionType");
    public static final AttributeKey<ConnectionSource> CONNECTION_SOURCE = AttributeKey.newInstance("ConnectionSource");

    private static final int LENGTH_FIELD_LENGTH = 4;
    private static final LengthFieldPrepender LENGTH_FIELD_PREPENDER = new LengthFieldPrepender(LENGTH_FIELD_LENGTH);
    private static final StreamableCodec MESSAGE_CODEC = new StreamableCodec();

    private final int localId;
    private final Bootstrap bootstrap;
    private final Set<BootstrapConnection> bootstrapConnections;
    private final Map<Integer, PeerConnection> peerConnections;
    private final long nodeTimestamp;
    private PeerDetails localDetails;
    private volatile boolean running = false;

    public ConnectionManager(int localId,
                             String[] bootstrapAddress,
                             EventLoopGroup eventLoopGroup,
                             List<String> localAddresses) {
        this.localId = localId;
        this.bootstrap = new Bootstrap()
                .group(eventLoopGroup)
                .channel(NioSocketChannel.class)
                .handler(this);
        this.nodeTimestamp = System.currentTimeMillis();
        this.bootstrapConnections = Collections.newSetFromMap(new ConcurrentHashMap<>());
        Arrays.stream(bootstrapAddress).map(BootstrapConnection::new).forEach(bootstrapConnections::add);
        // Use skip list map for predictable iteration order
        this.peerConnections = new ConcurrentSkipListMap<>();
        localDetails = new PeerDetails(localId, nodeTimestamp, nodeTimestamp, localAddresses);
    }

    public Optional<Message> poll() {
        final int numberOfConnections = peerConnections.size();
        if (numberOfConnections == 0) {
            return Optional.empty();
        }
        int skippedCount = 0;
        final int startAtIndex = roundRobinCounter.getAndUpdate(current -> (current + 1) % numberOfConnections);
        for (Map.Entry<Integer, PeerConnection> connection : peerConnections.entrySet()) {
            if (skippedCount < startAtIndex) {
                skippedCount++;
            } else {
                final Message message = connection.getValue().poll();
                if (message != null) {
                    return Optional.of(message);
                }
            }
        }
        for (Map.Entry<Integer, PeerConnection> connection : peerConnections.entrySet()) {
            if (skippedCount-- == 0) {
                return Optional.empty();
            }
            final Message message = connection.getValue().poll();
            if (message != null) {
                return Optional.of(message);
            }
        }
        return Optional.empty();
    }

    public Message pollFrom(int remoteId) {
        if (peerConnections.containsKey(remoteId)) {
            return peerConnections.get(remoteId).poll();
        }
        return null;
    }

    public Set<Integer> getConnectedNodeIds() {
        return peerConnections.entrySet().stream()
                .filter(e -> e.getValue().isConnected())
                .map(Map.Entry::getKey)
                .collect(Collectors.toUnmodifiableSet());
    }

    public void onBlock(int remoteId) {
        PeerConnection pc = peerConnections.get(remoteId);
        if (pc != null) {
            pc.disconnect();
        }
    }

    public void stop() {
        running = false;
        peerConnections.values()
                .forEach(PeerConnection::disconnect);
    }

    @Override
    public void close() throws IOException {
        stop();
        Closeables.closeQuietly(peerConnections.values());
    }

    public void onDisconnect(int remoteId, ChannelHandlerContext ctx) {
        PeerConnection pc = peerConnections.get(remoteId);
        if (pc != null) {
            pc.onDisconnect(ctx);
        }
    }

    public boolean shouldRead(int remoteId) {
        PeerConnection pc = peerConnections.get(remoteId);
        if (pc != null) {
            return pc.shouldRead();
        }
        return true;
    }

    public PeerDetails getLocalDetails() {
        return localDetails;
    }

    public void start() {
        LOGGER.info("Starting node {}", localId);
        running = true;
        peerConnections.values().forEach(PeerConnection::reset);
        peerConnections.values().forEach(PeerConnection::connect);
        connectToPending("starting node");
    }

    private void connectToPending(String reason) {
        if (!running) {
            return;
        }
        for (BootstrapConnection bootstrapConnection : bootstrapConnections) {
            startConnection(bootstrapConnection, reason);
        }
        for (PeerConnection peerConnection : peerConnections.values()) {
            startConnection(peerConnection, reason);
        }
    }

    private void startConnection(ConnectionSource connectionSource, String reason) {
        Iterator<String> nextConnectAttempt = connectionSource.getNextConnectAttempt();
        if (nextConnectAttempt != null) {
            new ConnectionAttempt(nextConnectAttempt, connectionSource, reason).startNextAttempt();
        }
    }

    public PeerConnection tryHandshake(Channel channel, PeerDetails peerDetails) {
        if (peerDetails.id() == localId) {
            // Don't establish a connection to ourselves
            channel.writeAndFlush(BlockMessage.INSTANCE);
            return null;
        }
        PeerConnection pc = peerConnections.computeIfAbsent(peerDetails.id(), PeerConnection::new);
        if (pc.tryHandshake(channel, peerDetails)) {
            sendGossipMessage(peerDetails.id());
            // initiate an outbound connection to the new peer
            connectToPending("handshake complete");
            return pc;
        }
        return null;
    }

    public void processGossipMessage(int sourceId, GossipMessage gossipMessage) {
        LOGGER.trace("Received gossip message from {}: {}", sourceId, gossipMessage);
        final boolean anyUpdated = gossipMessage.peerDetails()
                .stream()
                .filter(peer -> peer.id() != localId)
                .map(pd -> peerConnections.computeIfAbsent(pd.id(), PeerConnection::new).updatePeerDetails(pd))
                .map(PeerConnection.DetailsUpdateResult::isNew)
                .reduce(false, Boolean::logicalOr);
        if (anyUpdated) {
            broadcastGossipMessage();
            connectToPending("gossip received");
        }
    }

    public void sendGossipMessage(int remoteId) {
        final PeerConnection pc = peerConnections.get(remoteId);
        if (pc != null) {
            pc.send(createGossipMessage());
        }
    }

    private void broadcastGossipMessage() {
        final GossipMessage gossipMessage = createGossipMessage();
        peerConnections.values().forEach(pc -> pc.send(gossipMessage));
    }

    private GossipMessage createGossipMessage() {
        List<PeerDetails> allDetails = peerConnections.values().stream()
                .filter(PeerConnection::isConnected)
                .map(PeerConnection::getPeerDetails)
                .filter(Objects::nonNull)
                .toList();
        return new GossipMessage(allDetails);
    }

    @Override
    protected void initChannel(Channel ch) {
        ch.attr(CONNECTION_MANAGER).set(this);
        ch.pipeline()
                .addLast(LENGTH_FIELD_PREPENDER)
                .addLast(new LengthFieldBasedFrameDecoder(1 << 20, 0, LENGTH_FIELD_LENGTH, 0, 4))
                .addLast(MESSAGE_CODEC)
                .addLast(new ConnectionStateMachine());
    }

    public boolean hasConnectionTo(int remoteId) {
        final PeerConnection pc = peerConnections.get(remoteId);
        return pc != null && pc.isConnected();
    }

    public boolean send(int remoteId, Streamable message) {
        final PeerConnection pc = peerConnections.get(remoteId);
        if (pc != null) {
            return pc.send(message);
        }
        return false;
    }

    @Override
    public String toString() {
        return "ConnectionManager{" +
                "peerConnections=" + peerConnections +
                '}';
    }

    class BootstrapConnection implements ConnectionSource {

        private final List<String> address;
        private long nextAttemptTimestamp;
        private long nextIntervalMs = INITIAL_DELAY_MS;
        private boolean hasOutstanding = false;

        BootstrapConnection(String address) {
            this.address = List.of(address);
        }

        @Override
        public synchronized Iterator<String> getNextConnectAttempt() {
            if (!hasOutstanding && nextAttemptTimestamp < System.currentTimeMillis()) {
                hasOutstanding = true;
                return address.iterator();
            }
            return null;
        }

        @Override
        public void onSuccess() {
            bootstrapConnections.remove(this);
        }

        @Override
        public synchronized void onFailed(boolean retryable) {
            if (retryable) {
                nextAttemptTimestamp = System.currentTimeMillis() + nextIntervalMs;
                nextIntervalMs = Math.min((long) (nextIntervalMs * DELAY_GROWTH_RATE), MAX_DELAY_MS);
                hasOutstanding = false;
            } else {
                bootstrapConnections.remove(this);
            }
        }

        @Override
        public String toString() {
            return String.format("Bootstrap address %s", address);
        }
    }

    public interface ConnectionSource {

        Iterator<String> getNextConnectAttempt();

        void onSuccess();

        void onFailed(boolean retryable);
    }

    private class ConnectionAttempt {

        private final Iterator<String> addresses;
        private final ConnectionSource source;
        private final String reason;
        private String currentAddress;

        private ConnectionAttempt(Iterator<String> addresses, ConnectionSource source, String reason) {
            this.source = source;
            if (!addresses.hasNext()) {
                throw new IllegalArgumentException("List of addresses must not be empty");
            }
            this.addresses = addresses;
            this.reason = reason;
        }

        public void startNextAttempt() {
            if (!addresses.hasNext()) {
                source.onFailed(true);
                return;
            }
            currentAddress = addresses.next();
            bootstrap.register().addListener(this::channelRegistered);
        }

        private void channelRegistered(Future<? super Void> future) {
            final Channel channel = ((ChannelFuture) future).channel();
            channel.attr(CONNECTION_TYPE).set(ConnectionType.OUTBOUND);
            channel.attr(CONNECTION_SOURCE).set(source);
            LOGGER.trace("Attempting to connect to {}", currentAddress);
            channel.connect(NetworkUtil.resolveFirst(currentAddress)).addListener(this::connectEnded);
        }

        private void connectEnded(Future<? super Void> future) {
            ChannelFuture channelFuture = (ChannelFuture) future;
            if (channelFuture.isSuccess()) {
                LOGGER.info("Successfully connected to {} (source={}, reason={})", currentAddress, source, reason);
            } else {
                LOGGER.info("Failed to connect to {} (source={}, reason={})", currentAddress, source, reason);
                channelFuture.channel().close();
                startNextAttempt();
            }
        }
    }
}
