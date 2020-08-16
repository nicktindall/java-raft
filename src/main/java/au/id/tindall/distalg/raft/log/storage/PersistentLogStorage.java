package au.id.tindall.distalg.raft.log.storage;

import au.id.tindall.distalg.raft.log.entries.LogEntry;
import au.id.tindall.distalg.raft.log.persistence.EntrySerializer;
import au.id.tindall.distalg.raft.log.persistence.JavaEntrySerializer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.SYNC;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.Collections.unmodifiableList;


public class PersistentLogStorage implements LogStorage {

    private final EntrySerializer entrySerializer;
    private final FileChannel fileChannel;
    private List<Long> entryEndIndex;

    public PersistentLogStorage(Path logFilePath) {
        this(logFilePath, JavaEntrySerializer.INSTANCE);
    }

    public PersistentLogStorage(Path logFilePath, EntrySerializer entrySerializer) {
        this.entrySerializer = entrySerializer;
        try {
            fileChannel = FileChannel.open(logFilePath, READ, WRITE, CREATE, SYNC);
        } catch (IOException e) {
            throw new RuntimeException("Error opening log file for writing", e);
        }
        reIndex();
    }

    @Override
    public void add(LogEntry entry) {
        writeEntry(entry);
    }

    @Override
    public void truncate(int fromIndex) {
        try {
            fileChannel.truncate(entryEndIndex.get(fromIndex - 1));
            entryEndIndex = entryEndIndex.subList(0, fromIndex);
        } catch (IOException ex) {
            throw new RuntimeException("Error truncating log", ex);
        }
    }

    @Override
    public LogEntry getEntry(int index) {
        return readEntry(index);
    }

    @Override
    public List<LogEntry> getEntries() {
        List<LogEntry> entries = new ArrayList<>();
        for (int i = 0; i < size(); i++) {
            entries.add(readEntry(i + 1));
        }
        return unmodifiableList(entries);
    }

    @Override
    public List<LogEntry> getEntries(int fromIndexInclusive, int toIndexExclusive) {
        List<LogEntry> entries = new ArrayList<>();
        for (int i = fromIndexInclusive; i < toIndexExclusive; i++) {
            entries.add(readEntry(i));
        }
        return unmodifiableList(entries);
    }

    @Override
    public int size() {
        return entryEndIndex.size() - 1;
    }

    private long startPositionOfEntry(int index) {
        return entryEndIndex.get(index - 1);
    }

    private int lengthOfEntry(int index) {
        return (int) (entryEndIndex.get(index) - startPositionOfEntry(index) - 4);
    }

    public void reIndex() {
        try {
            entryEndIndex = new ArrayList<>();
            entryEndIndex.add(0L);
            fileChannel.position(0);
            ByteBuffer lengthBuffer = ByteBuffer.allocate(4);
            while (fileChannel.position() < fileChannel.size()) {
                lengthBuffer.position(0);
                fileChannel.read(lengthBuffer);
                int length = lengthBuffer.getInt(0);
                long endIndex = fileChannel.position() + length;
                entryEndIndex.add(endIndex);
                fileChannel.position(endIndex);
            }
        } catch (IOException ex) {
            throw new RuntimeException("Error reading log", ex);
        }
    }

    private void writeEntry(LogEntry entry) {
        try {
            byte[] entryBytes = entrySerializer.serialize(entry);
            ByteBuffer byteBuffer = ByteBuffer.allocate(entryBytes.length + 4);
            byteBuffer.putInt(entryBytes.length);
            byteBuffer.put(entryBytes);
            byteBuffer.position(0);
            fileChannel.write(byteBuffer);
            entryEndIndex.add(fileChannel.position());
        } catch (IOException ex) {
            throw new RuntimeException("Error writing to log", ex);
        }
    }

    private LogEntry readEntry(int index) {
        try {
            long offset = startPositionOfEntry(index);
            int length = lengthOfEntry(index);
            ByteBuffer buffer = ByteBuffer.allocate(length);
            fileChannel.read(buffer, offset + 4);
            return entrySerializer.deserialize(buffer.array());
        } catch (IOException ex) {
            throw new RuntimeException("Error reading log entry", ex);
        }
    }
}
