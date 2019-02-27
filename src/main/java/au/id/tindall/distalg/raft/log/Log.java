package au.id.tindall.distalg.raft.log;

import static java.lang.String.format;
import static java.util.Collections.unmodifiableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class Log {

    private List<LogEntry> entries;

    public Log() {
        entries = new ArrayList<>();
    }

    public void appendEntries(int prevLogIndex, List<LogEntry> newEntries) {
        if (prevLogIndex != 0 && !hasEntry(prevLogIndex)) {
            throw new IllegalArgumentException(format("Attempted to append after index %s when length is %s", prevLogIndex, entries.size()));
        }
        for (int offset = 0; offset < newEntries.size(); offset++) {
            int appendIndex = prevLogIndex + 1 + offset;
            appendEntry(appendIndex, newEntries.get(offset));
        }
    }

    private void appendEntry(int appendIndex, LogEntry entry) {
        if (hasEntry(appendIndex)) {
            if (!getEntry(appendIndex).getTerm().equals(entry.getTerm())) {
                entries = truncate(appendIndex);
                entries.add(entry);
            }
        } else {
            entries.add(entry);
        }
    }

    public boolean containsPreviousEntry(int prevLogIndex, Term prevLogTerm) {
        return hasEntry(prevLogIndex) && getEntry(prevLogIndex).getTerm().equals(prevLogTerm);
    }

    public List<LogEntry> getEntries() {
        return unmodifiableList(entries);
    }

    public int getLastLogIndex() {
        return entries.size();
    }

    public Optional<Term> getLastLogTerm() {
        return entries.size() > 0 ?
                Optional.of(entries.get(entries.size() - 1).getTerm())
                : Optional.empty();
    }

    public LogSummary getSummary() {
        return new LogSummary(getLastLogTerm(), getLastLogIndex());
    }

    public boolean hasEntry(int index) {
        if (index < 1) {
            throw new IllegalArgumentException("Log indices start at 1");
        }
        return entries.size() >= index;
    }

    public LogEntry getEntry(int index) {
        if (index < 1) {
            throw new IllegalArgumentException("Log indices start at 1");
        }
        return entries.get(index - 1);
    }

    private List<LogEntry> truncate(int fromIndex) {
        return new ArrayList<>(entries.subList(0, fromIndex - 1));
    }
}
