package au.id.tindall.distalg.raft.serialisation;

public enum LongIDSerializer implements IDSerializer {
    INSTANCE;

    @Override
    public void writeId(Object id, StreamingOutput streamingOutput) {
        streamingOutput.writeLong((Long) id);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T readId(StreamingInput streamingInput) {
        return (T) Long.valueOf(streamingInput.readLong());
    }
}
