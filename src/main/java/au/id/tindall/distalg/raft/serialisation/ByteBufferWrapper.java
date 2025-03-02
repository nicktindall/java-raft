package au.id.tindall.distalg.raft.serialisation;

public interface ByteBufferWrapper {

    void position(int position);

    int position();

    void put(byte value);

    void putInt(int value);

    void putLong(long value);

    void putDouble(double value);

    void put(byte[] value);

    byte get(int position);

    int getInt(int position);

    long getLong(int position);

    double getDouble(int position);

    void get(int position, byte[] bytes);

    byte[] contentAsBytes();
}
