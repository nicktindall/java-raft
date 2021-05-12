package au.id.tindall.distalg.raft.log;

import au.id.tindall.distalg.raft.log.entries.LogEntry;

public interface EntryAppendedEventHandler {

    void entryAppended(int index, LogEntry logEntry);
}
