package au.id.tindall.distalg.raft.log.storage;

import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.LogEntry;
import au.id.tindall.distalg.raft.log.persistence.EntrySerializer;
import au.id.tindall.distalg.raft.log.persistence.JavaEntrySerializer;
import au.id.tindall.distalg.raft.state.Snapshot;
import au.id.tindall.distalg.raft.util.IOUtil;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static au.id.tindall.distalg.raft.util.Closeables.closeQuietly;
import static java.lang.String.format;
import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.SYNC;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.Collections.unmodifiableList;
import static org.apache.logging.log4j.LogManager.getLogger;


public class PersistentLogStorage implements LogStorage {

    private static final Logger LOGGER = getLogger();
    private static final int DEFAULT_TRUNCATION_BUFFER = 20;

    private final EntrySerializer entrySerializer;
    private final Path logFilePath;
    private final AtomicInteger nextIndex = new AtomicInteger(1);
    private final int truncationBuffer;
    private FileChannel logFileChannel;
    private List<Long> entryEndIndex;
    private volatile int prevIndex = 0;
    private Term prevTerm = Term.ZERO;

    public PersistentLogStorage(Path logFilePath) {
        this(logFilePath, DEFAULT_TRUNCATION_BUFFER, JavaEntrySerializer.INSTANCE);
    }

    public PersistentLogStorage(Path logFilePath, int truncationBuffer) {
        this(logFilePath, truncationBuffer, JavaEntrySerializer.INSTANCE);
    }

    public PersistentLogStorage(Path logFilePath, int truncationBuffer, EntrySerializer entrySerializer) {
        this.logFilePath = logFilePath;
        this.entrySerializer = entrySerializer;
        this.truncationBuffer = truncationBuffer;
        try {
            logFileChannel = FileChannel.open(logFilePath, READ, WRITE, CREATE, SYNC);
        } catch (IOException e) {
            throw new RuntimeException("Error opening log file for writing", e);
        }
        reIndex();
    }

    @Override
    public void add(int appendIndex, LogEntry entry) {
        if (appendIndex != nextIndex.get()) {
            throw new IllegalArgumentException(format("Attempted to append to index %,d when next index is %,d", appendIndex, nextIndex.get()));
        }
        entryEndIndex.add(writeEntry(logFileChannel, entry));
    }

    @Override
    public void truncate(int fromIndex) {
        try {
            logFileChannel.truncate(startPositionOfEntry(fromIndex));
            entryEndIndex = entryEndIndex.subList(0, fromIndex - getPrevIndex());
            nextIndex.set(fromIndex);
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
    public synchronized void installSnapshot(Snapshot snapshot) {
        copyTailOfLog(snapshot);
        prevTerm = snapshot.getLastTerm();
    }

    private void copyTailOfLog(Snapshot snapshot) {
        long startTime = System.nanoTime();
        try {
            Path tempLogFilePath = Path.of(logFilePath.toAbsolutePath() + ".tmp");
            Files.deleteIfExists(tempLogFilePath);
            final int firstIndexInLog = Math.max(snapshot.getLastIndex() + 1 - truncationBuffer, getFirstLogIndex());
            try (final FileChannel tempFileLogChannel = FileChannel.open(tempLogFilePath, CREATE_NEW, WRITE, READ, SYNC)) {
                nextIndex.set(firstIndexInLog);
                LOGGER.debug("Truncating {} entries from the start of the log (to index={})", firstIndexInLog - prevIndex - 1, firstIndexInLog);
                for (int i = firstIndexInLog; i <= getLastLogIndex(); i++) {
                    writeEntry(tempFileLogChannel, getEntry(i));
                }
            }
            closeQuietly(logFileChannel);
            Files.move(tempLogFilePath, logFilePath, ATOMIC_MOVE, REPLACE_EXISTING);
            logFileChannel = FileChannel.open(logFilePath, READ, WRITE, CREATE, SYNC);
            reIndex(firstIndexInLog);
            prevIndex = firstIndexInLog - 1;
        } catch (IOException e) {
            throw new IllegalStateException("Couldn't truncate log to install snapshot!", e);
        }
        LOGGER.debug("Took {}us to truncate head of log", (System.nanoTime() - startTime) / 1_000);
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

    private long startPositionOfEntry(int index) {
        return endPositionOfEntry(index - 1);
    }

    private int lengthOfEntry(int index) {
        validateIndex(index);
        return (int) (endPositionOfEntry(index) - startPositionOfEntry(index) - 8);
    }

    private long endPositionOfEntry(int index) {
        if (index == 0) {
            return 0;
        }
        return entryEndIndex.get(index - getPrevIndex());
    }

    public void reIndex() {
        reIndex(1);
    }

    public void reIndex(int firstIndex) {
        int nextIndex = firstIndex;
        try {
            entryEndIndex = new ArrayList<>();
            entryEndIndex.add(0L);
            logFileChannel.position(0);
            while (logFileChannel.position() < logFileChannel.size()) {
                int index = IOUtil.readInteger(logFileChannel);
                if (nextIndex > 1 && index != nextIndex) {
                    throw new IllegalStateException(format("Corrupt log file detected! (expected sequence %,d, found sequence %,d)", nextIndex, index));
                }
                if (nextIndex == 1 && index != 1) {
                    firstIndex = index;
                }
                nextIndex = index + 1;
                int length = IOUtil.readInteger(logFileChannel);
                long endIndex = logFileChannel.position() + length;
                entryEndIndex.add(endIndex);
                logFileChannel.position(endIndex);
            }
            this.prevIndex = firstIndex - 1;
            this.nextIndex.set(nextIndex);
        } catch (IOException ex) {
            throw new RuntimeException("Error reading log", ex);
        }
    }

    private long writeEntry(FileChannel fileChannel, LogEntry entry) {
        try {
            byte[] entryBytes = entrySerializer.serialize(entry);
            IOUtil.writeValues(fileChannel, buf -> {
                buf.putInt(nextIndex.getAndIncrement());
                buf.putInt(entryBytes.length);
                buf.put(entryBytes);
            });
            return fileChannel.position();
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
            logFileChannel.read(buffer, offset + 8);
            return entrySerializer.deserialize(buffer.array());
        } catch (IOException ex) {
            throw new RuntimeException("Error reading log entry", ex);
        }
    }
}
