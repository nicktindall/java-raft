package au.id.tindall.distalg.raft.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.function.Consumer;

public class IOUtil {

    private static final ThreadLocal<ByteBuffer> valueTransferBuffer = ThreadLocal.withInitial(() -> ByteBuffer.allocate(2048));

    public static int readInteger(FileChannel fileChannel) throws IOException {
        final int i = readInteger(fileChannel, fileChannel.position());
        fileChannel.position(fileChannel.position() + 4);
        return i;
    }

    public static int readInteger(FileChannel fileChannel, long offset) throws IOException {
        final ByteBuffer intBuffer = valueTransferBuffer.get().clear().limit(4);
        final int read = fileChannel.read(intBuffer, offset);
        if (read != 4) {
            throw new IOException("Couldn't read int from FileChannel (read " + read + " bytes)");
        }
        intBuffer.flip();
        return intBuffer.getInt();
    }

    public static void writeByte(FileChannel fileChannel, long offset, byte b) throws IOException {
        fileChannel.write(valueTransferBuffer.get()
                .clear()
                .put(b)
                .flip(), offset);
    }

    public static void writeInteger(FileChannel fileChannel, long offset, int i) throws IOException {
        fileChannel.write(valueTransferBuffer.get()
                .clear()
                .putInt(i)
                .flip(), offset);
    }

    public static void writeLong(FileChannel fileChannel, long offset, long l) throws IOException {
        fileChannel.write(valueTransferBuffer.get()
                .clear()
                .putLong(l)
                .flip(), offset);
    }

    public static void writeValues(FileChannel fileChannel, Consumer<ByteBuffer> populator) throws IOException {
        final ByteBuffer clear = valueTransferBuffer.get().clear();
        populator.accept(clear);
        fileChannel.write(clear.flip());
    }
}
