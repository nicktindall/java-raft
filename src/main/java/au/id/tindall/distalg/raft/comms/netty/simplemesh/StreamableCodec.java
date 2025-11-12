package au.id.tindall.distalg.raft.comms.netty.simplemesh;

import au.id.tindall.distalg.raft.comms.netty.ByteBufIO;
import au.id.tindall.distalg.raft.serialisation.IntegerIDSerializer;
import au.id.tindall.distalg.raft.serialisation.Streamable;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.ReferenceCountUtil;

/**
 * Parses and serializes messages that implement {@link au.id.tindall.distalg.raft.serialisation.Streamable}
 */
@ChannelHandler.Sharable
public class StreamableCodec extends ChannelDuplexHandler {
    private static final ThreadLocal<ByteBufIO> BYTE_BUF_IO_THREAD_LOCAL = ThreadLocal.withInitial(ByteBufIO::new);

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof ByteBuf bb) {
            try {
                super.channelRead(ctx, BYTE_BUF_IO_THREAD_LOCAL.get().init(bb, IntegerIDSerializer.INSTANCE).readStreamable());
            } finally {
                ReferenceCountUtil.release(bb);
            }
        } else {
            ctx.fireChannelRead(msg);
        }
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof Streamable streamable) {
            ByteBuf buffer = ctx.alloc().buffer();
            try {
                BYTE_BUF_IO_THREAD_LOCAL.get().init(buffer, IntegerIDSerializer.INSTANCE).writeStreamable(streamable);
            } catch (Exception e) {
                ReferenceCountUtil.release(buffer);
                throw e;
            }
            super.write(ctx, buffer, promise);
        } else {
            super.write(ctx, msg, promise);
        }
    }
}
