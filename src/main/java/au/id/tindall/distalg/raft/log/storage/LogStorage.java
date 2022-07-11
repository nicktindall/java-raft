package au.id.tindall.distalg.raft.log.storage;

import au.id.tindall.distalg.raft.log.EntryStatus;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.LogEntry;
import au.id.tindall.distalg.raft.state.Snapshot;

import java.util.List;
import java.util.Optional;

import static java.lang.String.format;

public interface LogStorage {

    void add(LogEntry logEntry);

    void truncate(int fromIndex);

    default EntryStatus hasEntry(int index) {
        if (index < getFirstLogIndex()) {
            return EntryStatus.BeforeStart;
        } else if (index > getLastLogIndex()) {
            return EntryStatus.AfterEnd;
        }
        return EntryStatus.Present;
    }

    LogEntry getEntry(int index);

    List<LogEntry> getEntries();

    List<LogEntry> getEntries(int fromIndexInclusive, int toIndexExclusive);

    default int getFirstLogIndex() {
        return getPrevIndex() + 1;
    }

    default int getLastLogIndex() {
        return getFirstLogIndex() + size();
    }

    default int getNextLogIndex() {
        return getLastLogIndex() + 1;
    }

    /**
     * prevIndex is the index of the last discarded entry (initialized to 0 on first boot)
     *
     * @return the prevIndex
     */
    default int getPrevIndex() {
        return 0;
    }

    /**
     * prevTerm is the term of the last discarded entry (initialized to 0 on first boot)
     *
     * @return the prevTerm
     */
    default Term getPrevTerm() {
        return Term.ZERO;
    }

    void installSnapshot(Snapshot snapshot);

    default Optional<Term> getLastLogTerm() {
        return isEmpty() ?
                Optional.empty()
                : Optional.of(getEntry(getLastLogIndex()).getTerm());
    }

    default boolean isEmpty() {
        return size() == 0;
    }

    int size();

    default void validateIndex(int logIndex) {
        final EntryStatus entryStatus = hasEntry(logIndex);
        switch (entryStatus) {
            case Present:
                return;
            case BeforeStart:
                throw new ArrayIndexOutOfBoundsException(format("Index has been truncated by log compaction (%,d <= %,d)", logIndex, getPrevIndex()));
            case AfterEnd:
                throw new ArrayIndexOutOfBoundsException(format("Index is after end of log (%,d > %,d)", logIndex, getLastLogIndex()));
            default:
                throw new IllegalStateException("Unexpected entry status " + entryStatus);
        }
    }
}
