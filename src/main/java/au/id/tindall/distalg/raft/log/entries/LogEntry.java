package au.id.tindall.distalg.raft.log.entries;

import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.serialisation.Streamable;
import au.id.tindall.distalg.raft.serialisation.StreamingInput;
import au.id.tindall.distalg.raft.serialisation.StreamingOutput;

public abstract class LogEntry implements Streamable {

    private final Term term;

    protected LogEntry(Term term) {
        this.term = term;
    }

    protected LogEntry(StreamingInput streamingInput) {
        this.term = streamingInput.readStreamable();
    }

    public Term getTerm() {
        return term;
    }

    @Override
    public void writeTo(StreamingOutput streamingOutput) {
        streamingOutput.writeStreamable(term);
    }

    @Override
    public String toString() {
        return "LogEntry{" +
                "term=" + term +
                '}';
    }
}
