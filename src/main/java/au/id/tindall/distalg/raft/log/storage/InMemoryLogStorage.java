package au.id.tindall.distalg.raft.log.storage;

import au.id.tindall.distalg.raft.log.EntryStatus;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.LogEntry;
import au.id.tindall.distalg.raft.state.Snapshot;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.unmodifiableList;
import static java.util.List.copyOf;

public class InMemoryLogStorage implements LogStorage {

    private List<LogEntry> entries;

    private int prevIndex = 0;
    private Term prevTerm = Term.ZERO;

    public InMemoryLogStorage() {
        this.entries = new ArrayList<>();
    }

    @Override
    public void add(LogEntry logEntry) {
        this.entries.add(logEntry);
    }

    @Override
    public void truncate(int fromIndex) {
        validateIndex(fromIndex);
        entries = new ArrayList<>(entries.subList(0, toListIndex(fromIndex)));
    }

    @Override
    public LogEntry getEntry(int index) {
        validateIndex(index);
        return entries.get(toListIndex(index));
    }

    @Override
    public List<LogEntry> getEntries(int fromIndexInclusive, int toIndexExclusive) {
        validateIndex(fromIndexInclusive);
        validateIndex(toIndexExclusive - 1);
        return copyOf(entries.subList(toListIndex(fromIndexInclusive), toListIndex(toIndexExclusive)));
    }

    @Override
    public void installSnapshot(Snapshot snapshot) {
        int oldPrevIndex = prevIndex;
        prevIndex = snapshot.getLastIndex();
        prevTerm = snapshot.getLastTerm();
        if (oldPrevIndex != prevIndex) {
            entries = entries.subList(prevIndex - oldPrevIndex, entries.size());
        }
    }

    @Override
    public int getPrevIndex() {
        return prevIndex;
    }

    @Override
    public Term getPrevTerm() {
        return prevTerm;
    }

    @Override
    public EntryStatus hasEntry(int index) {
        final int listIndex = toListIndex(index);
        if (listIndex < 0) {
            return EntryStatus.BeforeStart;
        } else if (listIndex < entries.size()) {
            return EntryStatus.Present;
        } else {
            return EntryStatus.AfterEnd;
        }
    }

    @Override
    public List<LogEntry> getEntries() {
        return unmodifiableList(entries);
    }

    @Override
    public int size() {
        return entries.size();
    }

    private int toListIndex(int logIndex) {
        return logIndex - prevIndex - 1;
    }
}
