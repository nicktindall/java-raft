package au.id.tindall.distalg.raft.log.storage;

import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.LogEntry;

import java.util.List;
import java.util.Optional;

public interface LogStorage {

    void add(LogEntry logEntry);

    void truncate(int fromIndex);

    default boolean hasEntry(int index) {
        return size() >= index;
    }

    LogEntry getEntry(int index);

    List<LogEntry> getEntries();

    List<LogEntry> getEntries(int fromIndexInclusive, int toIndexExclusive);

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
