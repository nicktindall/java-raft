package au.id.tindall.distalg.raft.log.entries;

import au.id.tindall.distalg.raft.log.Term;

import java.io.Serializable;

public class LogEntry implements Serializable {

    private final Term term;

    public LogEntry(Term term) {
        this.term = term;
    }

    public Term getTerm() {
        return term;
    }

    @Override
    public String toString() {
        return "LogEntry{" +
                "term=" + term +
                '}';
    }
}
