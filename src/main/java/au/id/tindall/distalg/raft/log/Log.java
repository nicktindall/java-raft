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

import static java.lang.String.format;
import static java.util.stream.IntStream.range;
import static org.apache.logging.log4j.LogManager.getLogger;

public class Log implements SnapshotInstalledListener {

    private static final Logger LOGGER = getLogger();

    private final List<EntryCommittedEventHandler> entryCommittedEventHandlers;
    private final List<EntryAppendedEventHandler> entryAppendedEventHandlers;
    private final List<CommitIndexAdvancedHandler> commitIndexAdvancedHandlers;
    private final LogStorage storage;
    private int commitIndex;

    public Log() {
        this(new InMemoryLogStorage());
        LOGGER.warn("You're using a log with in-memory storage, this better not be production!");
    }

    public Log(LogStorage logStorage) {
        storage = logStorage;
        commitIndex = 0;
        entryCommittedEventHandlers = new ArrayList<>();
        entryAppendedEventHandlers = new ArrayList<>();
        commitIndexAdvancedHandlers = new ArrayList<>();
    }

    public Optional<Integer> updateCommitIndex(List<Integer> matchIndices, Term currentTerm) {
        int clusterSize = matchIndices.size() + 1;
        List<Integer> matchIndicesFromTheCurrentTermHigherThanTheCommitIndexInAscendingOrder = matchIndices.stream()
                .filter(index -> index > commitIndex)
                .filter(index -> getEntry(index).getTerm().equals(currentTerm))
                .sorted()
                .toList();
        int majorityThreshold = clusterSize / 2;
        if (matchIndicesFromTheCurrentTermHigherThanTheCommitIndexInAscendingOrder.size() + 1 > majorityThreshold) {
            Integer newCommitIndex = matchIndicesFromTheCurrentTermHigherThanTheCommitIndexInAscendingOrder
                    .get(matchIndicesFromTheCurrentTermHigherThanTheCommitIndexInAscendingOrder.size() - majorityThreshold);
            advanceCommitIndex(newCommitIndex);
            return Optional.of(newCommitIndex);
        }
        return Optional.empty();
    }

    public void appendEntries(int prevLogIndex, List<LogEntry> newEntries) {
        if (prevLogIndex != 0 && hasEntry(prevLogIndex) == EntryStatus.AFTER_END) {
            throw new IllegalArgumentException(format("Attempted to append after index %s when length is %s", prevLogIndex, storage.size()));
        }
        for (int offset = 0; offset < newEntries.size(); offset++) {
            int appendIndex = prevLogIndex + 1 + offset;
            appendEntry(appendIndex, newEntries.get(offset));
        }
    }

    private void appendEntry(int appendIndex, LogEntry entry) {
        final EntryStatus entryStatus = hasEntry(appendIndex);
        switch (entryStatus) {
            case BEFORE_START:
                throw new AttemptToAppendBeforeCommitIndexException(appendIndex, null, entry);
            case PRESENT:
                final LogEntry entryAtIndex = getEntry(appendIndex);
                if (entryAtIndex.getTerm().equals(entry.getTerm())) {
                    // We already have this entry
                    break;
                } else {
                    // The entry we have at that index was from a different term
                    if (appendIndex <= this.commitIndex) {
                        throw new AttemptToAppendBeforeCommitIndexException(appendIndex, entryAtIndex, entry);
                    }
                    storage.truncate(appendIndex);
                }
                // fallthrough
            case AFTER_END:
                storage.add(appendIndex, entry);
                notifyAppendedListeners(appendIndex);
                break;
            default:
                throw new IllegalStateException("Unknown entry status: " + entryStatus);
        }
    }

    class AttemptToAppendBeforeCommitIndexException extends IllegalStateException {
        public AttemptToAppendBeforeCommitIndexException(int appendIndex, LogEntry entryAtIndex, LogEntry entry) {
            super(format("Attempt made to truncate prior to commit index, this is a bug. appendIndex=%,d, commitIndex=%,d, prevIndex=%,d, entryAtIndex=%s, entry=%s",
                    appendIndex, getCommitIndex(), getPrevIndex(), entryAtIndex, entry));
        }
    }

    public boolean containsPreviousEntry(int prevLogIndex, Term prevLogTerm) {
        switch (hasEntry(prevLogIndex)) {
            case PRESENT:
                return getEntry(prevLogIndex).getTerm().equals(prevLogTerm);
            case BEFORE_START:
                return storage.getPrevIndex() == prevLogIndex && storage.getPrevTerm().equals(prevLogTerm);
            default:
                return false;
        }
    }

    public List<LogEntry> getEntries() {
        return storage.getEntries();
    }

    public int getLastLogIndex() {
        return storage.getLastLogIndex();
    }

    public int getNextLogIndex() {
        return storage.getNextLogIndex();
    }

    public int getPrevIndex() {
        return storage.getPrevIndex();
    }

    public Term getPrevTerm() {
        return storage.getPrevTerm();
    }

    public Optional<Term> getLastLogTerm() {
        return storage.getLastLogTerm();
    }

    public LogSummary getSummary() {
        return new LogSummary(getLastLogTerm(), getLastLogIndex());
    }

    public EntryStatus hasEntry(int index) {
        validateIndex(index);
        return storage.hasEntry(index);
    }

    public LogEntry getEntry(int index) {
        validateIndex(index);
        return storage.getEntry(index);
    }

    public List<LogEntry> getEntries(int index, int limit) {
        validateIndex(index);
        int toIndex = Math.min(storage.getNextLogIndex(), index + limit);
        return storage.getEntries(index, toIndex);
    }

    public int getCommitIndex() {
        return commitIndex;
    }

    public void advanceCommitIndex(int newCommitIndex) {
        if (newCommitIndex > this.commitIndex) {
            range(this.commitIndex, newCommitIndex)
                    .map(i -> i + 1)
                    .forEach(this::notifyCommittedListeners);
            this.commitIndex = newCommitIndex;
            notifyCommitIndexAdvancedListener(newCommitIndex);
        }
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

    public void addCommitIndexAdvancedEventHandler(CommitIndexAdvancedHandler eventHandler) {
        commitIndexAdvancedHandlers.add(eventHandler);
    }

    public void removeCommitIndexAdvancedEventHandler(CommitIndexAdvancedHandler eventHandler) {
        commitIndexAdvancedHandlers.remove(eventHandler);
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

    private void notifyCommitIndexAdvancedListener(int appendedIndex) {
        commitIndexAdvancedHandlers
                .forEach(handler -> handler.commitIndexAdvanced(appendedIndex));
    }

    private void validateIndex(int index) {
        if (index < 1) {
            throw new IllegalArgumentException("Log indices start at 1");
        }
    }

    @Override
    public void onSnapshotInstalled(Snapshot snapshot) {
        if (snapshot.getLastIndex() > commitIndex) {
            LOGGER.debug("Advancing commit index from {} to {} due to snapshot", commitIndex, snapshot.getLastIndex());
            this.commitIndex = snapshot.getLastIndex();
        }
    }
}
