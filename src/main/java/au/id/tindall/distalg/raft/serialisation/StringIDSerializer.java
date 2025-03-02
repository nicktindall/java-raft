package au.id.tindall.distalg.raft.serialisation;

public enum StringIDSerializer implements IDSerializer {
    INSTANCE;

    @Override
    public void writeId(Object id, StreamingOutput streamingOutput) {
        streamingOutput.writeString((String) id);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T readId(StreamingInput streamingInput) {
        return (T) streamingInput.readString();
    }
}
