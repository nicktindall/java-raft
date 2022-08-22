package au.id.tindall.distalg.raft.log.persistence;

import au.id.tindall.distalg.raft.log.entries.LogEntry;

import static au.id.tindall.distalg.raft.util.SerializationUtil.deserializeObject;
import static au.id.tindall.distalg.raft.util.SerializationUtil.serializeObject;

public class JavaEntrySerializer implements EntrySerializer {

    public static final JavaEntrySerializer INSTANCE = new JavaEntrySerializer();

    private JavaEntrySerializer() {
    }

    @Override
    public byte[] serialize(LogEntry entry) {
        return serializeObject(entry);
    }

    @Override
    public LogEntry deserialize(byte[] entryBytes) {
        return deserializeObject(entryBytes);
    }
}
