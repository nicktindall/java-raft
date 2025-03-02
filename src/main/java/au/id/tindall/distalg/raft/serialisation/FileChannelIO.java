package au.id.tindall.distalg.raft.serialisation;

import au.id.tindall.distalg.raft.util.BufferUtil;
import au.id.tindall.distalg.raft.util.UnsafeUtil;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class FileChannelIO implements StreamingInput, StreamingOutput {

    private static final Charset CHARSET = StandardCharsets.UTF_8;
    private static final int PAGE_SIZE = UnsafeUtil.get().pageSize();
    private final FileChannel fileChannel;
    private final FileChannel.MapMode mapMode;
    private final IDSerializer idSerializer;
    private final int chunkSize;
    private Chunk currentChunk;
    private int readPosition;
    private int writePosition;

    public FileChannelIO(IDSerializer idSerializer, FileChannel fileChannel, FileChannel.MapMode mapMode, int chunkSizeBytes) {
        this.idSerializer = idSerializer;
        this.fileChannel = fileChannel;
        this.mapMode = mapMode;
        this.chunkSize = align(chunkSizeBytes, RoundingMode.CEIL);
    }

    @Override
    public boolean readBoolean() {
        ensureMapped(readPosition, readPosition + 1);
        return currentChunk.readBoolean(readPosition++);
    }

    @Override
    public int readInteger() {
        ensureMapped(readPosition, readPosition + Integer.BYTES);
        final int value = currentChunk.readInteger(readPosition);
        readPosition += Integer.BYTES;
        return value;
    }

    @Override
    public long readLong() {
        ensureMapped(readPosition, readPosition + Long.BYTES);
        final long value = currentChunk.readLong(readPosition);
        readPosition += Long.BYTES;
        return value;
    }

    @Override
    public double readDouble() {
        ensureMapped(readPosition, readPosition + Double.BYTES);
        final double value = currentChunk.readDouble(readPosition);
        readPosition += Double.BYTES;
        return value;
    }

    @Override
    public String readString() {
        return new String(readBytes(), CHARSET);
    }

    @Override
    public byte[] readBytes() {
        final int bytesLength = currentChunk.readInteger(readPosition);
        ensureMapped(readPosition, readPosition + bytesLength + Integer.BYTES);
        final byte[] bytes = currentChunk.readBytes(readPosition);
        readPosition += Integer.BYTES + bytesLength;
        return bytes;
    }

    @Override
    public <T> T readIdentifier() {
        return idSerializer.readId(this);
    }

    @Override
    public void writeBoolean(boolean value) {
        ensureMapped(writePosition, writePosition + 1);
        currentChunk.writeBoolean(writePosition++, value);
    }

    @Override
    public void writeInteger(int value) {
        ensureMapped(writePosition, writePosition + Integer.BYTES);
        currentChunk.writeInteger(writePosition, value);
        writePosition += Integer.BYTES;
    }

    @Override
    public void writeLong(long value) {
        ensureMapped(writePosition, writePosition + Long.BYTES);
        currentChunk.writeLong(writePosition, value);
        writePosition += Long.BYTES;
    }

    @Override
    public void writeDouble(double value) {
        ensureMapped(writePosition, writePosition + Double.BYTES);
        currentChunk.writeDouble(writePosition, value);
        writePosition += Double.BYTES;
    }

    @Override
    public void writeString(String value) {
        writeBytes(value.getBytes(CHARSET));
    }

    @Override
    public void writeBytes(byte[] value) {
        ensureMapped(writePosition, writePosition + value.length + Integer.BYTES);
        currentChunk.writeBytes(writePosition, value);
        writePosition += value.length + Integer.BYTES;
    }

    @Override
    public void writeIdentifier(Object value) {
        idSerializer.writeId(value, this);
    }

    private void ensureMapped(int startPosition, int endPosition) {
        if (currentChunk == null) {
            map(startPosition, endPosition);
        } else {
            if (startPosition < currentChunk.startPosition
                    || endPosition >= currentChunk.endPosition) {
                map(startPosition, endPosition);
            }
        }
    }

    private void map(int startPosition, int endPosition) {
        if (startPosition - endPosition > chunkSize) {
            throw new IllegalArgumentException("Can't write more than " + chunkSize + " bytes");
        }
        if (currentChunk != null) {
            currentChunk.close();
            currentChunk = null;
        }
        final int mapStartPosition = align(startPosition, RoundingMode.FLOOR);
        final int mapEndPosition = Math.max(mapStartPosition + chunkSize, align(endPosition, RoundingMode.CEIL));
        try {
            final int mapSize = mapEndPosition - mapStartPosition;
            currentChunk = new Chunk(mapStartPosition, mapEndPosition, fileChannel.map(mapMode, mapStartPosition, mapSize));
        } catch (IOException e) {
            throw new UncheckedIOException("Error mapping chunk", e);
        }
    }

    private int align(int position, RoundingMode roundingMode) {
        if (position % PAGE_SIZE == 0) {
            return position;
        }
        return roundingMode.round(position);
    }

    enum RoundingMode {
        CEIL {
            @Override
            int round(int value) {
                return value - (value % PAGE_SIZE) + PAGE_SIZE;
            }
        },
        FLOOR {
            @Override
            int round(int value) {
                return value - (value % PAGE_SIZE);
            }
        };

        abstract int round(int value);
    }

    private record Chunk(int startPosition, int endPosition, MappedByteBuffer mappedByteBuffer) implements Closeable {

        public boolean readBoolean(int position) {
            return mappedByteBuffer.get(position - startPosition) == 1;
        }

        public void writeBoolean(int position, boolean value) {
            mappedByteBuffer.put(position - startPosition, (byte) (value ? 1 : 0));
        }

        public int readInteger(int position) {
            return mappedByteBuffer.getInt(position - startPosition);
        }

        public void writeInteger(int position, int value) {
            mappedByteBuffer.putInt(position - startPosition, value);
        }

        public long readLong(int position) {
            return mappedByteBuffer.getLong(position - startPosition);
        }

        public void writeLong(int position, long value) {
            mappedByteBuffer.putLong(position - startPosition, value);
        }

        public double readDouble(int position) {
            return mappedByteBuffer.getDouble(position - startPosition);
        }

        public void writeDouble(int position, double value) {
            mappedByteBuffer.putDouble(position - startPosition, value);
        }

        public byte[] readBytes(int position) {
            final int length = mappedByteBuffer.getInt(position - startPosition);
            final byte[] bytes = new byte[length];
            mappedByteBuffer.get(position - startPosition + Integer.BYTES, bytes);
            return bytes;
        }

        public void writeBytes(int position, byte[] value) {
            mappedByteBuffer.putInt(position - startPosition, value.length);
            mappedByteBuffer.put(position - startPosition + Integer.BYTES, value);
        }

        @Override
        public void close() {
            BufferUtil.free(mappedByteBuffer);
        }
    }
}
