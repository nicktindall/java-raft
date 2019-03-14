package au.id.tindall.distalg.raft.log;

import au.id.tindall.distalg.raft.log.entries.LogEntry;

public interface EntryCommittedEventHandler {

    void entryCommitted(int index, LogEntry logEntry);
}
