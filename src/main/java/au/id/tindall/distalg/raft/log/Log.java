package au.id.tindall.distalg.raft.log;

import au.id.tindall.distalg.raft.log.entries.LogEntry;
import au.id.tindall.distalg.raft.log.storage.InMemoryLogStorage;
import au.id.tindall.distalg.raft.log.storage.LogStorage;
import au.id.tindall.distalg.raft.state.Snapshot;
import au.id.tindall.distalg.raft.state.SnapshotInstalledListener;
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

public class Log implements SnapshotInstalledListener {

    private static final Logger LOGGER = getLogger();

    private final ReadWriteLock readWriteLock;
    private final List<EntryCommittedEventHandler> entryCommittedEventHandlers;
    private final List<EntryAppendedEventHandler> entryAppendedEventHandlers;
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
        entryAppendedEventHandlers = new ArrayList<>();
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
            if (prevLogIndex != 0 && hasEntry(prevLogIndex) == EntryStatus.AfterEnd) {
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
        final EntryStatus entryStatus = hasEntry(appendIndex);
        switch (entryStatus) {
            case BeforeStart:
                throwAttemptToAppendBeforeCommitIndexError(appendIndex, null, entry);
            case Present:
                final LogEntry entryAtIndex = getEntry(appendIndex);
                if (entryAtIndex.getTerm().equals(entry.getTerm())) {
                    // We already have this entry
                    break;
                } else {
                    // The entry we have at that index was from a different term
                    if (appendIndex <= this.commitIndex) {
                        throwAttemptToAppendBeforeCommitIndexError(appendIndex, entryAtIndex, entry);
                    }
                    storage.truncate(appendIndex);
                }
            case AfterEnd:
                storage.add(appendIndex, entry);
                notifyAppendedListeners(appendIndex);
                break;
            default:
                throw new IllegalStateException("Unknown entry status: " + entryStatus);
        }
    }

    private void throwAttemptToAppendBeforeCommitIndexError(int appendIndex, LogEntry entryAtIndex, LogEntry entry) {
        throw new IllegalStateException(format("Attempt made to truncate prior to commit index, this is a bug. appendIndex=%,d, commitIndex=%,d, prevIndex=%,d, entryAtIndex=%s, entry=%s",
                appendIndex, getCommitIndex(), getPrevIndex(), entryAtIndex, entry));
    }

    public boolean containsPreviousEntry(int prevLogIndex, Term prevLogTerm) {
        return doRead(() -> {
            switch (hasEntry(prevLogIndex)) {
                case Present:
                    return getEntry(prevLogIndex).getTerm().equals(prevLogTerm);
                case BeforeStart:
                    return storage.getPrevIndex() == prevLogIndex && storage.getPrevTerm().equals(prevLogTerm);
                default:
                    return false;
            }
        });
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

    public int getPrevIndex() {
        return doRead(storage::getPrevIndex);
    }

    public Term getPrevTerm() {
        return doRead(storage::getPrevTerm);
    }

    public Optional<Term> getLastLogTerm() {
        return doRead(storage::getLastLogTerm);
    }

    public LogSummary getSummary() {
        return doRead(() -> new LogSummary(getLastLogTerm(), getLastLogIndex()));
    }

    public EntryStatus hasEntry(int index) {
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
                        .forEach(this::notifyCommittedListeners);
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

    public void addEntryAppendedEventHandler(EntryAppendedEventHandler eventHandler) {
        entryAppendedEventHandlers.add(eventHandler);
    }

    public void removeEntryAppendedEventHandler(EntryAppendedEventHandler eventHandler) {
        entryAppendedEventHandlers.remove(eventHandler);
    }

    private void notifyCommittedListeners(int committedIndex) {
        final LogEntry entry = getEntry(committedIndex);
        entryCommittedEventHandlers
                .forEach(handler -> handler.entryCommitted(committedIndex, entry));
    }

    private void notifyAppendedListeners(int appendedIndex) {
        entryAppendedEventHandlers
                .forEach(handler -> handler.entryAppended(appendedIndex, getEntry(appendedIndex)));
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

    @Override
    public void onSnapshotInstalled(Snapshot snapshot) {
        doWrite(() -> {
            LOGGER.debug("Advancing commit index from {} to {} due to snapshot", commitIndex, snapshot.getLastIndex());
            this.commitIndex = Math.max(commitIndex, snapshot.getLastIndex());
            return null;
        });
    }
}
