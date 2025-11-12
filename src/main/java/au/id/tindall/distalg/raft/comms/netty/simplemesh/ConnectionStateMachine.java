package au.id.tindall.distalg.raft.comms.netty.simplemesh;

import au.id.tindall.distalg.raft.comms.netty.simplemesh.messages.BlockMessage;
import au.id.tindall.distalg.raft.comms.netty.simplemesh.messages.GossipMessage;
import au.id.tindall.distalg.raft.comms.netty.simplemesh.messages.HandshakeMessage;
import au.id.tindall.distalg.raft.comms.netty.simplemesh.messages.PayloadMessage;
import au.id.tindall.distalg.raft.comms.netty.simplemesh.messages.PeerDetails;
import au.id.tindall.distalg.raft.comms.netty.simplemesh.messages.StateChangeInvoker;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.AttributeKey;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static au.id.tindall.distalg.raft.comms.netty.simplemesh.ConnectionManager.CONNECTION_MANAGER;
import static au.id.tindall.distalg.raft.comms.netty.simplemesh.ConnectionManager.CONNECTION_SOURCE;
import static au.id.tindall.distalg.raft.comms.netty.simplemesh.ConnectionManager.CONNECTION_TYPE;
import static au.id.tindall.distalg.raft.comms.netty.simplemesh.ConnectionManager.MESSAGE_RECEIVER;

public class ConnectionStateMachine extends ChannelDuplexHandler {

    private static final Logger LOGGER = LogManager.getLogger();
    public static final AttributeKey<Integer> REMOTE_ID_KEY = AttributeKey.valueOf("remoteId");

    private ConnectionState currentState;

    public ConnectionStateMachine() {
        this.currentState = ConnectionStateImpl.Connected;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        PeerDetails localDetails = ctx.channel().attr(CONNECTION_MANAGER).get().getLocalDetails();
        LOGGER.trace("Sending handshake message to {}", ctx.channel().remoteAddress());
        ctx.channel().writeAndFlush(new HandshakeMessage(localDetails));
        ctx.read();
        super.channelActive(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof StateChangeInvoker invoker) {
            updateState(invoker.apply(ctx, currentState));
        } else {
            ctx.fireChannelRead(msg);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        Integer remoteId = ctx.channel().attr(REMOTE_ID_KEY).get();
        ConnectionManager.ConnectionType connectionType = ctx.channel().attr(CONNECTION_TYPE).get();
        LOGGER.error("Uncaught exception type={}, remote={}", connectionType, remoteId, cause);
        super.exceptionCaught(ctx, cause);
    }

    @Override
    public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
        LOGGER.debug("Disconnecting {}", ctx.channel().remoteAddress());
        updateState(ConnectionStateImpl.Disconnecting);
        super.disconnect(ctx, promise);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        ConnectionManager connectionManager = ctx.channel().attr(CONNECTION_MANAGER).get();
        Integer remoteId = ctx.channel().attr(REMOTE_ID_KEY).get();
        if (connectionManager != null && remoteId != null) {
            connectionManager.onDisconnect(remoteId, ctx);
        }
        super.channelInactive(ctx);
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        updateState(currentState.channelReadComplete(ctx));
        super.channelReadComplete(ctx);
    }

    private void updateState(ConnectionState newState) {
        if (currentState != newState) {
            LOGGER.trace("State changed from {} to {}", currentState, newState);
            currentState = newState;
        }
    }

    enum ConnectionStateImpl implements ConnectionState {
        Connected {
            @Override
            public ConnectionState onHandshake(HandshakeMessage message, ChannelHandlerContext ctx) {
                ctx.channel().attr(REMOTE_ID_KEY).set(message.peerDetails().id());
                final ConnectionManager connectionManager = ctx.channel().attr(CONNECTION_MANAGER).get();
                final PeerConnection pc = connectionManager.tryHandshake(ctx.channel(), message.peerDetails());
                final ConnectionManager.ConnectionSource connectionSource = ctx.channel().attr(CONNECTION_SOURCE).get();
                if (pc != null) {
                    if (connectionSource != null) {
                        connectionSource.onSuccess();
                    }
                    return Handshaken;
                }
                if (connectionSource != null) {
                    connectionSource.onFailed(false);
                }
                return FailedHandshake;
            }
        },
        Handshaken {
            @Override
            public ConnectionState onPayloadMessage(PayloadMessage message, ChannelHandlerContext ctx) {
                int remoteId = ctx.channel().attr(REMOTE_ID_KEY).get();
                ctx.channel().attr(MESSAGE_RECEIVER).get().accept(new Message(remoteId, message.payload()));
                return this;
            }

            @Override
            public ConnectionState onGossip(GossipMessage message, ChannelHandlerContext ctx) {
                int remoteId = ctx.channel().attr(REMOTE_ID_KEY).get();
                ctx.channel().attr(CONNECTION_MANAGER).get().processGossipMessage(remoteId, message);
                return this;
            }

            @Override
            public ConnectionState channelReadComplete(ChannelHandlerContext ctx) {
                if (ctx.channel().attr(CONNECTION_MANAGER).get().shouldRead(ctx.channel().attr(REMOTE_ID_KEY).get())) {
                    ctx.read();
                } else {
                    LOGGER.debug("Buffer full, halting reads");
                }
                return this;
            }
        },
        FailedHandshake,
        Disconnecting
    }

    public interface ConnectionState {

        default ConnectionState onBlock(BlockMessage blockMessage, ChannelHandlerContext ctx) {
            ConnectionManager connectionManager = ctx.channel().attr(CONNECTION_MANAGER).get();
            Integer remoteId = ctx.channel().attr(REMOTE_ID_KEY).get();
            if (connectionManager != null && remoteId != null) {
                if (remoteId != connectionManager.getLocalDetails().id()) {
                    LOGGER.info("Got block message from {}", remoteId);
                }
                connectionManager.onBlock(remoteId);
            } else {
                LOGGER.warn("Got blocked by an unknown remote, address = {}", ctx.channel().remoteAddress());
            }
            return this;
        }

        default ConnectionState onHandshake(HandshakeMessage message, ChannelHandlerContext ctx) {
            LOGGER.warn("Unexpected handshake received. State: {}", name());
            return this;
        }

        default ConnectionState onPayloadMessage(PayloadMessage message, ChannelHandlerContext ctx) {
            LOGGER.warn("Unexpected payload message received, ignoring. State: {}", name());
            return this;
        }

        default ConnectionState onGossip(GossipMessage message, ChannelHandlerContext ctx) {
            LOGGER.warn("Unexpected gossip received, ignoring. State: {}", name());
            return this;
        }

        default ConnectionState channelReadComplete(ChannelHandlerContext ctx) {
            ctx.read();
            return this;
        }

        String name();
    }
}
