package au.id.tindall.distalg.raft.log;

import au.id.tindall.distalg.raft.log.entries.LogEntry;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static java.lang.String.format;
import static java.util.Collections.unmodifiableList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;

public class Log {

    private List<LogEntry> entries;
    private int commitIndex;
    private List<EntryCommittedEventHandler> entryCommittedEventHandlers;

    public Log() {
        entries = new ArrayList<>();
        commitIndex = 0;
        entryCommittedEventHandlers = new ArrayList<>();
    }

    public Optional<Integer> updateCommitIndex(List<Integer> matchIndices) {
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
    }

    public void appendEntries(int prevLogIndex, List<LogEntry> newEntries) {
        if (prevLogIndex != 0 && !hasEntry(prevLogIndex)) {
            throw new IllegalArgumentException(format("Attempted to append after index %s when length is %s", prevLogIndex, entries.size()));
        }
        for (int offset = 0; offset < newEntries.size(); offset++) {
            int appendIndex = prevLogIndex + 1 + offset;
            appendEntry(appendIndex, newEntries.get(offset));
        }
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
        return hasEntry(prevLogIndex) && getEntry(prevLogIndex).getTerm().equals(prevLogTerm);
    }

    public List<LogEntry> getEntries() {
        return unmodifiableList(entries);
    }

    public int getLastLogIndex() {
        return entries.size();
    }

    public int getNextLogIndex() {
        return getLastLogIndex() + 1;
    }

    public Optional<Term> getLastLogTerm() {
        return entries.isEmpty() ?
                Optional.empty()
                : Optional.of(entries.get(entries.size() - 1).getTerm());
    }

    public LogSummary getSummary() {
        return new LogSummary(getLastLogTerm(), getLastLogIndex());
    }

    public boolean hasEntry(int index) {
        if (index < 1) {
            throw new IllegalArgumentException("Log indices start at 1");
        }
        return entries.size() >= index;
    }

    public LogEntry getEntry(int index) {
        if (index < 1) {
            throw new IllegalArgumentException("Log indices start at 1");
        }
        return entries.get(index - 1);
    }

    public List<LogEntry> getEntries(int index, int limit) {
        if (index < 1) {
            throw new IllegalArgumentException("Log indices start at 1");
        }
        int toIndex = Math.min(entries.size(), index + limit - 1);
        return unmodifiableList(entries.subList(index - 1, toIndex));
    }

    private List<LogEntry> truncate(int fromIndex) {
        if (fromIndex <= this.commitIndex) {
            throw new IllegalStateException("Attempt made to truncate prior to commit index, this is a bug");
        }
        return new ArrayList<>(entries.subList(0, fromIndex - 1));
    }

    public int getCommitIndex() {
        return commitIndex;
    }

    public void advanceCommitIndex(int newCommitIndex) {
        if (newCommitIndex > this.commitIndex) {
            range(this.commitIndex, newCommitIndex)
                    .map(i -> i + 1)
                    .forEach(this::notifyListeners);
            this.commitIndex = newCommitIndex;
        }
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
}
