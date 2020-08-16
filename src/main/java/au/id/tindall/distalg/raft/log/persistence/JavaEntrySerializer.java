package au.id.tindall.distalg.raft.log.persistence;

import au.id.tindall.distalg.raft.log.entries.LogEntry;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class JavaEntrySerializer implements EntrySerializer {

    public static final JavaEntrySerializer INSTANCE = new JavaEntrySerializer();

    private JavaEntrySerializer() {
    }

    @Override
    public byte[] serialize(LogEntry entry) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
                oos.writeObject(entry);
            }
            return baos.toByteArray();
        } catch (IOException ex) {
            throw new RuntimeException("Error serializing entry", ex);
        }
    }

    @Override
    public LogEntry deserialize(byte[] entryBytes) {
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(entryBytes))) {
            return (LogEntry) ois.readObject();
        } catch (ClassNotFoundException | IOException ex) {
            throw new RuntimeException("Error deserializing entry", ex);
        }
    }
}
