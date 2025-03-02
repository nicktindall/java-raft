package au.id.tindall.distalg.raft.rpc.server;

import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.serialisation.Streamable;
import au.id.tindall.distalg.raft.serialisation.StreamingInput;
import au.id.tindall.distalg.raft.serialisation.StreamingOutput;

public abstract class RpcMessage<I> implements Streamable {

    private final Term term;
    private final I source;

    protected RpcMessage(Term term, I source) {
        this.term = term;
        this.source = source;
    }

    protected RpcMessage(StreamingInput streamingInput) {
        this(streamingInput.readStreamable(), streamingInput.readIdentifier());
    }

    public Term getTerm() {
        return term;
    }

    public I getSource() {
        return source;
    }

    @Override
    public void writeTo(StreamingOutput streamingOutput) {
        streamingOutput.writeStreamable(term);
        streamingOutput.writeIdentifier(source);
    }

    @Override
    public String toString() {
        return "RpcMessage{" +
                "term=" + term +
                ", source=" + source +
                '}';
    }
}
