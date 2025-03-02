package au.id.tindall.distalg.raft.serialisation;

public interface IDSerializer {

    void writeId(Object id, StreamingOutput streamingOutput);

    <T> T readId(StreamingInput streamingInput);
}
