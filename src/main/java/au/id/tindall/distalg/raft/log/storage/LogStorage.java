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

    int getLastLogIndex();

    int getNextLogIndex();

    default Optional<Term> getLastLogTerm() {
        return isEmpty() ?
                Optional.empty()
                : Optional.of(getEntry(getLastLogIndex()).getTerm());
    }

    boolean isEmpty();

    int size();
}
