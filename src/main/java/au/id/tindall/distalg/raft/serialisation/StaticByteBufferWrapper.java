package au.id.tindall.distalg.raft.serialisation;

import java.nio.ByteBuffer;

public class StaticByteBufferWrapper implements ByteBufferWrapper {

    protected ByteBuffer buffer;

    public StaticByteBufferWrapper(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    @Override
    public void position(int position) {
        buffer.position(position);
    }

    @Override
    public int position() {
        return buffer.position();
    }

    @Override
    public void put(byte value) {
        buffer.put(value);
    }

    @Override
    public void putInt(int value) {
        buffer.putInt(value);
    }

    @Override
    public void putLong(long value) {
        buffer.putLong(value);
    }

    @Override
    public void putDouble(double value) {
        buffer.putDouble(value);
    }

    @Override
    public void put(byte[] value) {
        buffer.put(value);
    }

    @Override
    public byte get(int position) {
        return buffer.get(position);
    }

    @Override
    public int getInt(int position) {
        return buffer.getInt(position);
    }

    @Override
    public long getLong(int position) {
        return buffer.getLong(position);
    }

    @Override
    public double getDouble(int position) {
        return buffer.getDouble(position);
    }

    @Override
    public void get(int position, byte[] bytes) {
        buffer.get(position, bytes);
    }

    @Override
    public byte[] contentAsBytes() {
        byte[] bytes = new byte[buffer.position()];
        buffer.get(0, bytes, 0, buffer.position());
        return bytes;
    }
}
