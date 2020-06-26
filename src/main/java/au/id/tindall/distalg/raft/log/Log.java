package au.id.tindall.distalg.raft.log;

import au.id.tindall.distalg.raft.log.entries.LogEntry;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

import static java.lang.String.format;
import static java.util.Collections.unmodifiableList;
import static java.util.List.copyOf;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;

public class Log {

    private final ReadWriteLock readWriteLock;
    private final List<EntryCommittedEventHandler> entryCommittedEventHandlers;
    private List<LogEntry> entries;
    private int commitIndex;

    public Log() {
        readWriteLock = new ReentrantReadWriteLock();
        entries = new ArrayList<>();
        commitIndex = 0;
        entryCommittedEventHandlers = new ArrayList<>();
    }

    public Optional<Integer> updateCommitIndex(List<Integer> matchIndices) {
        return doWrite(() -> {
            int clusterSize = matchIndices.size() + 1;
            List<Integer> matchIndicesHigherThanTheCommitIndexInAscendingOrder = matchIndices.stream()
                    .filter(index -> index > commitIndex)
                    .sorted()
                    .collect(toList());
            int majorityThreshold = clusterSize / 2;
            if (matchIndicesHigherThanTheCommitIndexInAscendingOrder.size() + 1 > majorityThreshold) {
                Integer newCommitIndex = matchIndicesHigherThanTheCommitIndexInAscendingOrder
                        .get(matchIndicesHigherThanTheCommitIndexInAscendingOrder.size() - majorityThreshold);
                advanceCommitIndex(newCommitIndex);
                return Optional.of(newCommitIndex);
            }
            return Optional.empty();
        });
    }

    public void appendEntries(int prevLogIndex, List<LogEntry> newEntries) {
        doWrite(() -> {
            if (prevLogIndex != 0 && !hasEntry(prevLogIndex)) {
                throw new IllegalArgumentException(format("Attempted to append after index %s when length is %s", prevLogIndex, entries.size()));
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
                entries = truncate(appendIndex);
                entries.add(entry);
            }
        } else {
            entries.add(entry);
        }
    }

    public boolean containsPreviousEntry(int prevLogIndex, Term prevLogTerm) {
        return doRead(() -> hasEntry(prevLogIndex) && getEntry(prevLogIndex).getTerm().equals(prevLogTerm));
    }

    public List<LogEntry> getEntries() {
        return doRead(() -> unmodifiableList(entries));
    }

    public int getLastLogIndex() {
        return doRead(() -> entries.size());
    }

    public int getNextLogIndex() {
        return doRead(() -> getLastLogIndex() + 1);
    }

    public Optional<Term> getLastLogTerm() {
        return doRead(() -> entries.isEmpty() ?
                Optional.empty()
                : Optional.of(entries.get(entries.size() - 1).getTerm()));
    }

    public LogSummary getSummary() {
        return doRead(() -> new LogSummary(getLastLogTerm(), getLastLogIndex()));
    }

    public boolean hasEntry(int index) {
        return doRead(() -> {
            validateIndex(index);
            return entries.size() >= index;
        });
    }

    public LogEntry getEntry(int index) {
        return doRead(() -> {
            validateIndex(index);
            return entries.get(index - 1);
        });
    }

    public List<LogEntry> getEntries(int index, int limit) {
        return doRead(() -> {
            validateIndex(index);
            int toIndex = Math.min(entries.size(), index + limit - 1);
            return copyOf(entries.subList(index - 1, toIndex));
        });
    }

    private List<LogEntry> truncate(int fromIndex) {
        if (fromIndex <= this.commitIndex) {
            throw new IllegalStateException("Attempt made to truncate prior to commit index, this is a bug");
        }
        return new ArrayList<>(entries.subList(0, fromIndex - 1));
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
