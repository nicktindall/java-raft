package au.id.tindall.distalg.raft.log.storage;

import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.LogEntry;
import au.id.tindall.distalg.raft.serialisation.ByteBufferIO;
import au.id.tindall.distalg.raft.serialisation.IDSerializer;
import au.id.tindall.distalg.raft.state.Snapshot;
import au.id.tindall.distalg.raft.util.IOUtil;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static au.id.tindall.distalg.raft.log.storage.BufferedTruncationCalculator.calculateTruncation;
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

    private final IDSerializer idSerializer;
    private final Path logFilePath;
    private final AtomicInteger nextIndex = new AtomicInteger(1);
    private final int truncationBuffer;
    private FileChannel logFileChannel;
    private List<Long> entryEndIndex;
    private volatile int prevIndex = 0;
    private Term prevTerm = Term.ZERO;

    public PersistentLogStorage(IDSerializer idSerializer, Path logFilePath) {
        this(idSerializer, logFilePath, DEFAULT_TRUNCATION_BUFFER);
    }

    public PersistentLogStorage(IDSerializer idSerializer, Path logFilePath, int truncationBuffer) {
        this.idSerializer = idSerializer;
        this.logFilePath = logFilePath;
        this.truncationBuffer = truncationBuffer;
        try {
            logFileChannel = FileChannel.open(logFilePath, READ, WRITE, CREATE, SYNC);
        } catch (IOException e) {
            throw new UncheckedIOException("Error opening log file for writing", e);
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
            throw new UncheckedIOException("Error truncating log", ex);
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
        long startTime = System.nanoTime();
        try {
            BufferedTruncationCalculator.TruncationDetails td = calculateTruncation(snapshot, this, truncationBuffer);
            int firstIndexInLog = td.getNewPrevIndex() + 1;
            if (td.getEntriesToTruncate() > 0) {
                Path tempLogFilePath = Path.of(logFilePath.toAbsolutePath() + ".tmp");
                Files.deleteIfExists(tempLogFilePath);
                try (final FileChannel tempFileLogChannel = FileChannel.open(tempLogFilePath, CREATE_NEW, WRITE, READ, SYNC)) {
                    nextIndex.set(firstIndexInLog);
                    LOGGER.debug("Truncating {} entries from the start of the log (to index={})", td.getEntriesToTruncate(), firstIndexInLog);
                    for (int i = firstIndexInLog; i <= getLastLogIndex(); i++) {
                        writeEntry(tempFileLogChannel, getEntry(i));
                    }
                }
                closeQuietly(logFileChannel);
                Files.move(tempLogFilePath, logFilePath, ATOMIC_MOVE, REPLACE_EXISTING);
                logFileChannel = FileChannel.open(logFilePath, READ, WRITE, CREATE, SYNC);
            }
            reIndex(firstIndexInLog);
            prevIndex = td.getNewPrevIndex();
            prevTerm = td.getNewPrevTerm();
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
        int localNextIndex = firstIndex;
        try {
            entryEndIndex = new ArrayList<>();
            entryEndIndex.add(0L);
            logFileChannel.position(0);
            while (logFileChannel.position() < logFileChannel.size()) {
                int index = IOUtil.readInteger(logFileChannel);
                if (localNextIndex > 1 && index != localNextIndex) {
                    throw new IllegalStateException(format("Corrupt log file detected! (expected sequence %,d, found sequence %,d)", localNextIndex, index));
                }
                if (localNextIndex == 1 && index != 1) {
                    firstIndex = index;
                }
                localNextIndex = index + 1;
                int length = IOUtil.readInteger(logFileChannel);
                long endIndex = logFileChannel.position() + length;
                entryEndIndex.add(endIndex);
                logFileChannel.position(endIndex);
            }
            this.prevIndex = firstIndex - 1;
            this.nextIndex.set(localNextIndex);
        } catch (IOException ex) {
            throw new UncheckedIOException("Error reading log", ex);
        }
    }

    private long writeEntry(FileChannel fileChannel, LogEntry entry) {
        try {
            ByteBuffer buffer = ByteBuffer.allocate(10240);
            ByteBufferIO.wrap(idSerializer, buffer).writeStreamable(entry);
            IOUtil.writeValues(fileChannel, buf -> {
                buf.putInt(nextIndex.getAndIncrement());
                buffer.flip();
                buf.putInt(buffer.remaining());
                buf.put(buffer);
            });
            return fileChannel.position();
        } catch (IOException ex) {
            throw new UncheckedIOException("Error writing to log", ex);
        }
    }

    private LogEntry readEntry(int index) {
        validateIndex(index);
        try {
            long offset = startPositionOfEntry(index);
            int length = lengthOfEntry(index);
            ByteBuffer buffer = ByteBuffer.allocate(length);
            logFileChannel.read(buffer, offset + 8);
            return ByteBufferIO.wrap(idSerializer, buffer).readStreamable();
        } catch (IOException ex) {
            throw new UncheckedIOException("Error reading log entry", ex);
        }
    }
}
