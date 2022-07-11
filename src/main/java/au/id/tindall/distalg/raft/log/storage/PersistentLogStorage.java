package au.id.tindall.distalg.raft.log.storage;

import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.LogEntry;
import au.id.tindall.distalg.raft.log.persistence.EntrySerializer;
import au.id.tindall.distalg.raft.log.persistence.JavaEntrySerializer;
import au.id.tindall.distalg.raft.state.Snapshot;

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
    private final FileChannel logFileChannel;
    private List<Long> entryEndIndex;

    private int prevIndex = 0;
    private Term prevTerm = Term.ZERO;

    public PersistentLogStorage(Path logFilePath) {
        this(logFilePath, JavaEntrySerializer.INSTANCE);
    }

    public PersistentLogStorage(Path logFilePath, EntrySerializer entrySerializer) {
        this.entrySerializer = entrySerializer;
        try {
            logFileChannel = FileChannel.open(logFilePath, READ, WRITE, CREATE, SYNC);
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
            logFileChannel.truncate(startPositionOfEntry(fromIndex));
            entryEndIndex = entryEndIndex.subList(0, fromIndex - getPrevIndex());
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
        for (int i = getFirstLogIndex(); i <= getLastLogIndex(); i++) {
            entries.add(readEntry(i));
        }
        return unmodifiableList(entries);
    }

    @Override
    public List<LogEntry> getEntries(int fromIndexInclusive, int toIndexExclusive) {
        validateIndex(fromIndexInclusive);
        validateIndex(toIndexExclusive - 1);
        List<LogEntry> entries = new ArrayList<>();
        for (int i = fromIndexInclusive; i < toIndexExclusive; i++) {
            entries.add(readEntry(i));
        }
        return unmodifiableList(entries);
    }

    @Override
    public void installSnapshot(Snapshot snapshot) {
        int oldPrevIndex = prevIndex;
        prevIndex = snapshot.getLastIndex();
        prevTerm = snapshot.getLastTerm();
        if (prevIndex < oldPrevIndex + entryEndIndex.size() - 1) {
            entryEndIndex = entryEndIndex.subList(prevIndex - oldPrevIndex, entryEndIndex.size());
        } else {
            entryEndIndex = entryEndIndex.subList(entryEndIndex.size() - 1, entryEndIndex.size());
        }
    }

    @Override
    public int getPrevIndex() {
        return prevIndex;
    }

    @Override
    public Term getPrevTerm() {
        return prevTerm;
    }

    @Override
    public int size() {
        return entryEndIndex.size() - 1;
    }

    @Override
    public int getLastLogIndex() {
        return getPrevIndex() + size();
    }

    private long startPositionOfEntry(int index) {
        return endPositionOfEntry(index - 1);
    }

    private int lengthOfEntry(int index) {
        validateIndex(index);
        return (int) (endPositionOfEntry(index) - startPositionOfEntry(index) - 4);
    }

    private long endPositionOfEntry(int index) {
        if (index == 0) {
            return 0;
        }
        return entryEndIndex.get(index - getPrevIndex());
    }

    public void reIndex() {
        try {
            entryEndIndex = new ArrayList<>();
            entryEndIndex.add(0L);
            logFileChannel.position(0);
            ByteBuffer lengthBuffer = ByteBuffer.allocate(4);
            while (logFileChannel.position() < logFileChannel.size()) {
                lengthBuffer.clear();
                logFileChannel.read(lengthBuffer);
                int length = lengthBuffer.getInt(0);
                long endIndex = logFileChannel.position() + length;
                entryEndIndex.add(endIndex);
                logFileChannel.position(endIndex);
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
            byteBuffer.flip();
            logFileChannel.write(byteBuffer);
            entryEndIndex.add(logFileChannel.position());
        } catch (IOException ex) {
            throw new RuntimeException("Error writing to log", ex);
        }
    }

    private LogEntry readEntry(int index) {
        validateIndex(index);
        try {
            long offset = startPositionOfEntry(index);
            int length = lengthOfEntry(index);
            ByteBuffer buffer = ByteBuffer.allocate(length);
            logFileChannel.read(buffer, offset + 4);
            return entrySerializer.deserialize(buffer.array());
        } catch (IOException ex) {
            throw new RuntimeException("Error reading log entry", ex);
        }
    }
}
