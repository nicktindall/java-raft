package au.id.tindall.distalg.raft.comms.netty.simplemesh.messages;

import au.id.tindall.distalg.raft.comms.netty.simplemesh.ConnectionStateMachine;
import io.netty.channel.ChannelHandlerContext;

public interface StateChangeInvoker {

    ConnectionStateMachine.ConnectionState apply(ChannelHandlerContext ctx,
                                                 ConnectionStateMachine.ConnectionState currentState);
}
