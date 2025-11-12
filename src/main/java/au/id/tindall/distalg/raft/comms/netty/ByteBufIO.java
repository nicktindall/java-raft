package au.id.tindall.distalg.raft.comms.netty;

import au.id.tindall.distalg.raft.serialisation.IDSerializer;
import au.id.tindall.distalg.raft.serialisation.StreamingInput;
import au.id.tindall.distalg.raft.serialisation.StreamingOutput;
import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;

public class ByteBufIO implements StreamingInput, StreamingOutput {

    private ByteBuf byteBuf;
    private IDSerializer idSerializer;

    public ByteBufIO init(ByteBuf byteBuf, IDSerializer idSerializer) {
        this.byteBuf = byteBuf;
        this.idSerializer = idSerializer;
        return this;
    }

    @Override
    public boolean readBoolean() {
        return byteBuf.readBoolean();
    }

    @Override
    public int readInteger() {
        return byteBuf.readInt();
    }

    @Override
    public long readLong() {
        return byteBuf.readLong();
    }

    @Override
    public double readDouble() {
        return byteBuf.readDouble();
    }

    @Override
    public String readString() {
        return new String(readBytes(), StandardCharsets.UTF_8);
    }

    @Override
    public byte[] readBytes() {
        int length = byteBuf.readInt();
        byte[] bytes = new byte[length];
        byteBuf.readBytes(bytes);
        return bytes;
    }

    @Override
    public <T> T readIdentifier() {
        return idSerializer.readId(this);
    }

    @Override
    public void writeBoolean(boolean value) {
        byteBuf.writeBoolean(value);
    }

    @Override
    public void writeInteger(int value) {
        byteBuf.writeInt(value);
    }

    @Override
    public void writeLong(long value) {
        byteBuf.writeLong(value);
    }

    @Override
    public void writeDouble(double value) {
        byteBuf.writeDouble(value);
    }

    @Override
    public void writeString(String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        writeBytes(bytes);
    }

    @Override
    public void writeBytes(byte[] value) {
        byteBuf.writeInt(value.length);
        byteBuf.writeBytes(value);
    }

    @Override
    public void writeIdentifier(Object value) {
        idSerializer.writeId(value, this);
    }
}
