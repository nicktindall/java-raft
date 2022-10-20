package au.id.tindall.distalg.raft.log.storage;

import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.LogEntry;
import au.id.tindall.distalg.raft.state.Snapshot;

import java.util.ArrayList;
import java.util.List;

import static au.id.tindall.distalg.raft.log.storage.BufferedTruncationCalculator.calculateTruncation;
import static java.lang.String.format;
import static java.util.Collections.unmodifiableList;
import static java.util.List.copyOf;

public class InMemoryLogStorage implements LogStorage {

    private final int truncationBuffer;
    private List<LogEntry> entries;

    private int prevIndex = 0;
    private Term prevTerm = Term.ZERO;

    public InMemoryLogStorage() {
        this(0);
    }

    public InMemoryLogStorage(int truncationBuffer) {
        this.truncationBuffer = truncationBuffer;
        this.entries = new ArrayList<>();
    }

    @Override
    public void add(int appendIndex, LogEntry logEntry) {
        if (toListIndex(appendIndex) != entries.size()) {
            throw new IllegalArgumentException(format("Attempting to append at index %,d when next index is %,d", appendIndex, entries.size() + prevIndex));
        }
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
        BufferedTruncationCalculator.TruncationDetails td = calculateTruncation(snapshot, this, truncationBuffer);
        if (td.getNewPrevIndex() != prevIndex) {
            if (td.getEntriesToTruncate() < entries.size()) {
                entries = entries.subList(td.getEntriesToTruncate(), entries.size());
            } else {
                entries = new ArrayList<>();
            }
            prevIndex = td.getNewPrevIndex();
            prevTerm = td.getNewPrevTerm();
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
