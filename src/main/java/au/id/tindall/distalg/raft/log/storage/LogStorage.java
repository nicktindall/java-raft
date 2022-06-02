package au.id.tindall.distalg.raft.log.storage;

import au.id.tindall.distalg.raft.log.EntryStatus;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.LogEntry;

import java.util.List;
import java.util.Optional;

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
        return 1;
    }

    default int getLastLogIndex() {
        return size();
    }

    default int getNextLogIndex() {
        return size() + 1;
    }

    default Optional<Term> getLastLogTerm() {
        return isEmpty() ?
                Optional.empty()
                : Optional.of(getEntry(getLastLogIndex()).getTerm());
    }

    default boolean isEmpty() {
        return size() == 0;
    }

    int size();
}
