package au.id.tindall.distalg.raft.serialisation;

public interface Streamable {

    MessageIdentifier getMessageIdentifier();

    void writeTo(StreamingOutput streamingOutput);
}
