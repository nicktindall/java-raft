package au.id.tindall.distalg.raft.comms.netty.simplemesh;

import au.id.tindall.distalg.raft.comms.netty.simplemesh.messages.PayloadMessage;
import au.id.tindall.distalg.raft.serialisation.Streamable;
import au.id.tindall.distalg.raft.threading.NamedThreadFactory;
import au.id.tindall.distalg.raft.util.Closeables;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.Future;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class Node implements Closeable {

    private static final Logger LOGGER = LogManager.getLogger();

    private final String localAddress;
    private final int localId;
    private final ConnectionManager connectionManager;
    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;
    private boolean closed = false;
    private Channel serverChannel;

    public Node(String localAddress, String[] initialRemoteAddresses, int localId) {
        this.localAddress = localAddress;
        this.localId = localId;
        this.bossGroup = new NioEventLoopGroup(1, NamedThreadFactory.forThreadGroup("node-" + localId + "-boss-group"));
        this.workerGroup = new NioEventLoopGroup(NamedThreadFactory.forThreadGroup("node-" + localId + "-worker-group"));
        this.connectionManager = new ConnectionManager(localId, initialRemoteAddresses, workerGroup, List.of(localAddress));
    }

    public Set<Integer> getServerIds() {
        return connectionManager.getConnectedNodeIds();
    }

    public int getLocalId() {
        return localId;
    }

    public synchronized void start() {
        if (closed) {
            throw new IllegalStateException("Already closed!");
        }
        try {
            ServerBootstrap serverBootstrap = new ServerBootstrap();
            serverBootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childAttr(ConnectionManager.CONNECTION_TYPE, ConnectionManager.ConnectionType.INBOUND)
                    .childHandler(connectionManager)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childOption(ChannelOption.AUTO_READ, false);

            // Bind and start to accept incoming connections.
            InetSocketAddress localInetAddress = NetworkUtil.resolveFirst(localAddress);
            serverChannel = serverBootstrap.bind(localInetAddress.getPort()).sync().channel();
            LOGGER.info("Opened server channel {}", serverChannel.localAddress());
            // Start establishing outgoing connections
            connectionManager.start();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Error running server", e);
        }
    }

    public Set<Integer> getConnectedNodeIds() {
        return connectionManager.getConnectedNodeIds();
    }

    public boolean isConnectedTo(int remoteId) {
        return connectionManager.hasConnectionTo(remoteId);
    }

    public boolean sendMessage(int remoteId, Streamable message) {
        return connectionManager.send(remoteId, new PayloadMessage(message));
    }

    public Optional<Message> poll() {
        return connectionManager.poll();
    }

    public Optional<Message> pollFrom(int remoteId) {
        return Optional.ofNullable(connectionManager.pollFrom(remoteId));
    }

    public synchronized void stop() {
        if (serverChannel != null) {
            serverChannel.deregister().syncUninterruptibly();
            serverChannel.close().syncUninterruptibly();
            serverChannel = null;
        }
        connectionManager.stop();
    }

    @Override
    public synchronized void close() {
        if (!closed) {
            closed = true;
            stop();
            Closeables.closeQuietly(connectionManager);
            blockingShutdown(workerGroup, bossGroup);
        }
    }

    private void blockingShutdown(EventLoopGroup... groups) {
        List<? extends Future<?>> shutdownFutures = Arrays.stream(groups)
                .map(elg -> elg.shutdownGracefully(0, 500, TimeUnit.MILLISECONDS))
                .toList();
        try {
            for (Future<?> future : shutdownFutures) {
                future.sync();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public String toString() {
        return "Node{" +
                "localAddress='" + localAddress + '\'' +
                ", localId=" + localId +
                ", connectionManager=" + connectionManager +
                '}';
    }
}
