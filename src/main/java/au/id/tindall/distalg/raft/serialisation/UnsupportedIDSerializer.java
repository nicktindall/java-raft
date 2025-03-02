package au.id.tindall.distalg.raft.serialisation;

public enum UnsupportedIDSerializer implements IDSerializer {
    INSTANCE;

    @Override
    public void writeId(Object id, StreamingOutput streamingOutput) {
        throw new UnsupportedOperationException("ID Serialization not supported");
    }

    @Override
    public <T> T readId(StreamingInput streamingInput) {
        throw new UnsupportedOperationException("ID Serialization not supported");
    }
}
