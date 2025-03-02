package au.id.tindall.distalg.raft.serialisation;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class ByteBufferIO implements StreamingOutput, StreamingInput {

    private static final Charset CHARSET = StandardCharsets.UTF_8;
    private final ByteBufferWrapper buffer;
    private final IDSerializer idSerializer;
    private int readPosition;

    public static ByteBufferIO elastic(IDSerializer idSerializer) {
        return new ByteBufferIO(new ElasticByteBufferWrapper(), idSerializer);
    }

    public static ByteBufferIO fixed(IDSerializer idSerializer, int size) {
        return new ByteBufferIO(new ElasticByteBufferWrapper(size), idSerializer);
    }

    public static ByteBufferIO wrap(IDSerializer idSerializer, ByteBuffer byteBuffer) {
        return new ByteBufferIO(new StaticByteBufferWrapper(byteBuffer), idSerializer);
    }

    public static ByteBufferIO wrap(IDSerializer idSerializer, byte[] byteArray) {
        return new ByteBufferIO(new StaticByteBufferWrapper(ByteBuffer.wrap(byteArray)), idSerializer);
    }

    private ByteBufferIO(ByteBufferWrapper buffer, IDSerializer idSerializer) {
        this.buffer = buffer;
        this.idSerializer = idSerializer;
    }

    public void setReadPosition(int readPosition) {
        this.readPosition = readPosition;
    }

    public void setWritePosition(int writePosition) {
        buffer.position(writePosition);
    }

    public int getWritePosition() {
        return buffer.position();
    }

    @Override
    public void writeBoolean(boolean value) {
        buffer.put((byte) (value ? 1 : 0));
    }

    @Override
    public void writeInteger(int value) {
        buffer.putInt(value);
    }

    @Override
    public void writeLong(long value) {
        buffer.putLong(value);
    }

    @Override
    public void writeDouble(double value) {
        buffer.putDouble(value);
    }

    @Override
    public void writeString(String value) {
        writeBytes(value.getBytes(CHARSET));
    }

    @Override
    public void writeBytes(byte[] value) {
        buffer.putInt(value.length);
        buffer.put(value);
    }

    @Override
    public void writeIdentifier(Object value) {
        idSerializer.writeId(value, this);
    }

    @Override
    public boolean readBoolean() {
        boolean result = buffer.get(readPosition) == 1;
        readPosition += Byte.BYTES;
        return result;
    }

    @Override
    public int readInteger() {
        int result = buffer.getInt(readPosition);
        readPosition += Integer.BYTES;
        return result;
    }

    @Override
    public long readLong() {
        long result = buffer.getLong(readPosition);
        readPosition += Long.BYTES;
        return result;
    }

    @Override
    public double readDouble() {
        double result = buffer.getDouble(readPosition);
        readPosition += Double.BYTES;
        return result;
    }

    @Override
    public String readString() {
        return new String(readBytes(), CHARSET);
    }

    @Override
    public byte[] readBytes() {
        final int length = buffer.getInt(readPosition);
        readPosition += Integer.BYTES;
        byte[] bytes = new byte[length];
        buffer.get(readPosition, bytes);
        readPosition += length;
        return bytes;
    }

    @Override
    public <T> T readIdentifier() {
        return idSerializer.readId(this);
    }

    public byte[] contentAsBytes() {
        return buffer.contentAsBytes();
    }
}
