package au.id.tindall.distalg.raft.log.storage;

import au.id.tindall.distalg.raft.log.entries.LogEntry;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.unmodifiableList;
import static java.util.List.copyOf;

public class ArrayListLogStorage implements LogStorage {

    List<LogEntry> entries;

    public ArrayListLogStorage() {
        this.entries = new ArrayList<>();
    }

    @Override
    public void add(LogEntry logEntry) {
        this.entries.add(logEntry);
    }

    @Override
    public void truncate(int fromIndex) {
        entries = new ArrayList<>(entries.subList(0, fromIndex - 1));
    }

    @Override
    public LogEntry getEntry(int index) {
        return entries.get(index - 1);
    }

    @Override
    public List<LogEntry> getEntries(int fromIndexInclusive, int toIndexExclusive) {
        return copyOf(entries.subList(fromIndexInclusive - 1, toIndexExclusive - 1));
    }

    @Override
    public boolean hasEntry(int index) {
        return entries.size() >= index;
    }

    @Override
    public List<LogEntry> getEntries() {
        return unmodifiableList(entries);
    }

    @Override
    public int size() {
        return entries.size();
    }
}
