package au.id.tindall.distalg.raft.log;

import java.io.Serializable;
import java.util.Arrays;

public class LogEntry implements Serializable {

    private final Term term;
    private final byte[] command;

    public LogEntry(Term term, byte[] command) {
        this.term = term;
        this.command = Arrays.copyOf(command, command.length);
    }

    public Term getTerm() {
        return term;
    }

    public byte[] getCommand() {
        return Arrays.copyOf(command, command.length);
    }

    @Override
    public String toString() {
        return "LogEntry{" +
                "term=" + term +
                ", command=(" + command.length + " bytes)" +
                '}';
    }
}
