package au.id.tindall.distalg.raft.serialisation;

import java.nio.ByteBuffer;

public class ElasticByteBufferWrapper extends StaticByteBufferWrapper {

    private static final int DEFAULT_INITIAL_SIZE = 1024;

    public ElasticByteBufferWrapper() {
        this(DEFAULT_INITIAL_SIZE);
    }

    public ElasticByteBufferWrapper(int initialSize) {
        super(ByteBuffer.allocate(initialSize));
    }

    @Override
    public void position(int position) {
        ensureCapacity(position + 1);
        super.position(position);
    }

    @Override
    public void put(byte value) {
        ensureAdditionalCapacity(Byte.BYTES);
        super.put(value);
    }

    @Override
    public void putInt(int value) {
        ensureAdditionalCapacity(Integer.BYTES);
        super.putInt(value);
    }

    @Override
    public void putLong(long value) {
        ensureAdditionalCapacity(Long.BYTES);
        super.putLong(value);
    }

    @Override
    public void putDouble(double value) {
        ensureAdditionalCapacity(Double.BYTES);
        super.putDouble(value);
    }

    @Override
    public void put(byte[] value) {
        ensureAdditionalCapacity(value.length);
        super.put(value);
    }

    private void ensureAdditionalCapacity(int additionalCapacity) {
        ensureCapacity(buffer.position() + additionalCapacity + 1);
    }

    private void ensureCapacity(int requiredCapacity) {
        if (buffer.capacity() < requiredCapacity) {
            final ByteBuffer original = buffer;
            final int originalPosition = original.position();
            final int newCapacity = requiredCapacity * 2;
            original.rewind();
            buffer = ByteBuffer.allocate(newCapacity);
            buffer.put(original);
            buffer.position(originalPosition);
        }
    }
}
