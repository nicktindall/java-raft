package au.id.tindall.distalg.raft.log.persistence;

import au.id.tindall.distalg.raft.log.entries.LogEntry;

public interface EntrySerializer {

    byte[] serialize(LogEntry entry);

    LogEntry deserialize(byte[] entryBytes);
}
