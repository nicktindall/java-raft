package au.id.tindall.distalg.raft.log;

import au.id.tindall.distalg.raft.log.entries.LogEntry;
import au.id.tindall.distalg.raft.log.storage.InMemoryLogStorage;
import au.id.tindall.distalg.raft.log.storage.LogStorage;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.apache.logging.log4j.LogManager.getLogger;

public class Log {

    private static final Logger LOGGER = getLogger();

    private final ReadWriteLock readWriteLock;
    private final List<EntryCommittedEventHandler> entryCommittedEventHandlers;
    private final LogStorage storage;
    private int commitIndex;

    public Log() {
        this(new InMemoryLogStorage());
        LOGGER.warn("You're using a log with in-memory storage, this better not be production!");
    }

    public Log(LogStorage logStorage) {
        readWriteLock = new ReentrantReadWriteLock();
        storage = logStorage;
        commitIndex = 0;
        entryCommittedEventHandlers = new ArrayList<>();
    }

    public Optional<Integer> updateCommitIndex(List<Integer> matchIndices, Term currentTerm) {
        return doWrite(() -> {
            int clusterSize = matchIndices.size() + 1;
            List<Integer> matchIndicesFromTheCurrentTermHigherThanTheCommitIndexInAscendingOrder = matchIndices.stream()
                    .filter(index -> index > commitIndex)
                    .filter(index -> getEntry(index).getTerm().equals(currentTerm))
                    .sorted()
                    .collect(toList());
            int majorityThreshold = clusterSize / 2;
            if (matchIndicesFromTheCurrentTermHigherThanTheCommitIndexInAscendingOrder.size() + 1 > majorityThreshold) {
                Integer newCommitIndex = matchIndicesFromTheCurrentTermHigherThanTheCommitIndexInAscendingOrder
                        .get(matchIndicesFromTheCurrentTermHigherThanTheCommitIndexInAscendingOrder.size() - majorityThreshold);
                advanceCommitIndex(newCommitIndex);
                return Optional.of(newCommitIndex);
            }
            return Optional.empty();
        });
    }

    public void appendEntries(int prevLogIndex, List<LogEntry> newEntries) {
        doWrite(() -> {
            if (prevLogIndex != 0 && !hasEntry(prevLogIndex)) {
                throw new IllegalArgumentException(format("Attempted to append after index %s when length is %s", prevLogIndex, storage.size()));
            }
            for (int offset = 0; offset < newEntries.size(); offset++) {
                int appendIndex = prevLogIndex + 1 + offset;
                appendEntry(appendIndex, newEntries.get(offset));
            }
            return null;
        });
    }

    private void appendEntry(int appendIndex, LogEntry entry) {
        if (hasEntry(appendIndex)) {
            if (!getEntry(appendIndex).getTerm().equals(entry.getTerm())) {
                if (appendIndex <= this.commitIndex) {
                    throw new IllegalStateException("Attempt made to truncate prior to commit index, this is a bug");
                }
                storage.truncate(appendIndex);
                storage.add(entry);
            }
        } else {
            storage.add(entry);
        }
    }

    public boolean containsPreviousEntry(int prevLogIndex, Term prevLogTerm) {
        return doRead(() -> hasEntry(prevLogIndex) && getEntry(prevLogIndex).getTerm().equals(prevLogTerm));
    }

    public List<LogEntry> getEntries() {
        return doRead(storage::getEntries);
    }

    public int getLastLogIndex() {
        return doRead(storage::getLastLogIndex);
    }

    public int getNextLogIndex() {
        return doRead(storage::getNextLogIndex);
    }

    public Optional<Term> getLastLogTerm() {
        return doRead(storage::getLastLogTerm);
    }

    public LogSummary getSummary() {
        return doRead(() -> new LogSummary(getLastLogTerm(), getLastLogIndex()));
    }

    public boolean hasEntry(int index) {
        return doRead(() -> {
            validateIndex(index);
            return storage.hasEntry(index);
        });
    }

    public LogEntry getEntry(int index) {
        return doRead(() -> {
            validateIndex(index);
            return storage.getEntry(index);
        });
    }

    public List<LogEntry> getEntries(int index, int limit) {
        return doRead(() -> {
            validateIndex(index);
            int toIndex = Math.min(storage.getNextLogIndex(), index + limit);
            return storage.getEntries(index, toIndex);
        });
    }

    public int getCommitIndex() {
        return doRead(() -> commitIndex);
    }

    public void advanceCommitIndex(int newCommitIndex) {
        doWrite(() -> {
            if (newCommitIndex > this.commitIndex) {
                range(this.commitIndex, newCommitIndex)
                        .map(i -> i + 1)
                        .forEach(this::notifyListeners);
                this.commitIndex = newCommitIndex;
            }
            return null;
        });
    }

    public void addEntryCommittedEventHandler(EntryCommittedEventHandler eventHandler) {
        entryCommittedEventHandlers.add(eventHandler);
    }

    public void removeEntryCommittedEventHandler(EntryCommittedEventHandler eventHandler) {
        entryCommittedEventHandlers.remove(eventHandler);
    }

    private void notifyListeners(int committedIndex) {
        entryCommittedEventHandlers
                .forEach(handler -> handler.entryCommitted(committedIndex, getEntry(committedIndex)));
    }

    private void validateIndex(int index) {
        if (index < 1) {
            throw new IllegalArgumentException("Log indices start at 1");
        }
    }

    private <V> V doRead(Supplier<V> function) {
        return acquireLockAnd(readWriteLock.readLock(), function);
    }

    private <V> V doWrite(Supplier<V> function) {
        return acquireLockAnd(readWriteLock.writeLock(), function);
    }

    private <V> V acquireLockAnd(Lock lock, Supplier<V> function) {
        lock.lock();
        try {
            return function.get();
        } finally {
            lock.unlock();
        }
    }
}
