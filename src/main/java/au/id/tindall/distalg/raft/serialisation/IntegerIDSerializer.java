package au.id.tindall.distalg.raft.serialisation;

public enum IntegerIDSerializer implements IDSerializer {
    INSTANCE;


    @Override
    public void writeId(Object id, StreamingOutput streamingOutput) {
        streamingOutput.writeInteger((Integer) id);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T readId(StreamingInput streamingInput) {
        return (T) Integer.valueOf(streamingInput.readInteger());
    }
}
