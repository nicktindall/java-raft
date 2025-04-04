package au.id.tindall.distalg.raft.log.storage;

import au.id.tindall.distalg.raft.log.EntryStatus;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.LogEntry;
import au.id.tindall.distalg.raft.serialisation.IDSerializer;
import au.id.tindall.distalg.raft.state.Snapshot;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static au.id.tindall.distalg.raft.log.storage.BufferedTruncationCalculator.calculateTruncation;
import static au.id.tindall.distalg.raft.util.Closeables.closeQuietly;
import static java.lang.String.format;
import static java.util.Collections.unmodifiableList;
import static org.apache.logging.log4j.LogManager.getLogger;


public class MemoryMappedLogStorage implements LogStorage, Closeable {

    private static final Logger LOGGER = getLogger();
    private static final int DEFAULT_TRUNCATION_BUFFER = 20;
    private static final Pattern ENTRY_BLOCK_PATTERN = Pattern.compile("[0-9A-Fa-f]{10}\\.log");
    private static final int DEFAULT_ENTRIES_PER_BLOCK = 10_000;

    private final IDSerializer idSerializer;
    private final Path logDirectoryPath;
    private final AtomicInteger nextIndex = new AtomicInteger(1);
    private final int truncationBuffer;
    private final int entriesPerBlock;
    private Term prevTerm = Term.ZERO;
    private Map<Integer, LogBlockHolder> logBlocks;
    private int prevIndex;

    public MemoryMappedLogStorage(IDSerializer idSerializer, Path logDirectoryPath) {
        this(idSerializer, logDirectoryPath, DEFAULT_TRUNCATION_BUFFER);
    }

    public MemoryMappedLogStorage(IDSerializer idSerializer, Path logDirectoryPath, int truncationBuffer) {
        this(idSerializer, DEFAULT_ENTRIES_PER_BLOCK, logDirectoryPath, truncationBuffer);
    }

    public MemoryMappedLogStorage(IDSerializer idSerializer, int entriesPerBlock, Path logDirectoryPath, int truncationBuffer) {
        this.idSerializer = idSerializer;
        this.entriesPerBlock = entriesPerBlock;
        this.logDirectoryPath = logDirectoryPath;
        this.truncationBuffer = truncationBuffer;
        this.logBlocks = new LinkedHashMap<>();
        loadExistingState();
    }

    @Override
    public void add(int appendIndex, LogEntry entry) {
        if (appendIndex != nextIndex.get()) {
            throw new IllegalArgumentException(format("Attempted to append to index %,d when next index is %,d", appendIndex, nextIndex.get()));
        }
        writeEntry(appendIndex, entry);
        nextIndex.incrementAndGet();
    }

    @Override
    public void truncate(int fromIndex) {
        final EntryStatus entryStatus = hasEntry(fromIndex);
        switch (entryStatus) {
            case PRESENT:
                int blockId = blockIdForIndex(fromIndex);
                logBlocks.get(blockId).getLogBlock().truncate(fromIndex);
                logBlocks.keySet().stream()
                        .filter(val -> val > blockId)
                        .forEach(val -> logBlocks.get(val).getLogBlock().truncate(fromIndex));
                nextIndex.set(fromIndex);
                break;
            case BEFORE_START:
                throw new IndexOutOfBoundsException("Can't truncate from before prevIndex (fromIndex=" + fromIndex + ")");
            case AFTER_END:
                throw new IndexOutOfBoundsException("Can't truncate from after last index (fromIndex=" + fromIndex + ")");
            default:
                throw new IllegalStateException("Unexpected EntryStatus " + entryStatus);
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
        BufferedTruncationCalculator.TruncationDetails td = calculateTruncation(snapshot, this, truncationBuffer);
        final List<Integer> blocksToDelete = logBlocks.entrySet().stream()
                .filter(val -> lastIndexInBlockId(val.getValue().blockId) <= td.getNewPrevIndex())
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
        if (!blocksToDelete.isEmpty()) {
            final Optional<Integer> firstBlockToRetain = logBlocks.entrySet().stream()
                    .filter(entry -> firstIndexInBlockId(entry.getValue().blockId) > td.getNewPrevIndex())
                    .map(Map.Entry::getKey)
                    .findFirst();
            if (firstBlockToRetain.isPresent()) {
                final LogBlockHolder firstBlockToRetainHolder = logBlocks.get(firstBlockToRetain.get());
                Term newPrevTerm = readEntry(firstBlockToRetainHolder.getLogBlock().getPrevIndex()).getTerm();
                firstBlockToRetainHolder.getLogBlock().setPrevTerm(newPrevTerm);
            }
            blocksToDelete.forEach(btd -> logBlocks.remove(btd).delete());
            if (logBlocks.isEmpty()) {
                final int blockIdForNewIndex = blockIdForIndex(td.getNewPrevIndex());
                final LogBlockHolder newFirstLogBlock = new LogBlockHolder(idSerializer, logDirectoryPath.resolve(filenameForPrevIndex(blockIdForNewIndex)), blockIdForNewIndex, td.getNewPrevIndex(), entriesPerBlock - (td.getNewPrevIndex() % entriesPerBlock));
                logBlocks.put(blockIdForNewIndex, newFirstLogBlock);
                newFirstLogBlock.getLogBlock().setPrevTerm(td.getNewPrevTerm());
                nextIndex.set(snapshot.getLastIndex() + 1);
            }
            LogBlock firstBlock = logBlocks.values().iterator().next().getLogBlock();
            prevIndex = firstBlock.getPrevIndex();
            prevTerm = firstBlock.getPrevTerm();
        }

        final long truncateDurationMicros = (System.nanoTime() - startTime) / 1_000;
        if (truncateDurationMicros > 500) {
            LOGGER.debug("Took {}us to truncate head of log", truncateDurationMicros);
        }
    }

    private int firstIndexInBlockId(int blockId) {
        if (blockId == 0) {
            return 1;
        }
        return lastIndexInBlockId(blockId - 1) + 1;
    }

    private int lastIndexInBlockId(int blockId) {
        return ((blockId + 1) * entriesPerBlock);
    }

    private LogEntry readEntry(int index) {
        final EntryStatus entryStatus = hasEntry(index);
        switch (entryStatus) {
            case PRESENT:
                int blockId = blockIdForIndex(index);
                final LogBlockHolder logBlockHolder = logBlocks.get(blockId);
                return logBlockHolder.getLogBlock().readEntry(index);
            case AFTER_END:
                throw new IndexOutOfBoundsException(format("Index is after end of log (%d > %d)", index, nextIndex.get() - 1));
            case BEFORE_START:
                throw new IndexOutOfBoundsException(format("Index has been truncated by log compaction (%d <= %d)", index, prevIndex));
            default:
                throw new IllegalStateException("Unexpected EntryStatus " + entryStatus);
        }
    }

    private void writeEntry(int index, LogEntry entry) {
        int blockId = blockIdForIndex(index);
        LogBlockHolder logBlockHolder = logBlocks.get(blockId);
        if (logBlockHolder == null) {
            logBlockHolder = new LogBlockHolder(idSerializer, logDirectoryPath.resolve(filenameForPrevIndex(blockId)), blockId, index - 1, entriesPerBlock);
            logBlocks.put(blockId, logBlockHolder);
        }
        logBlockHolder.getLogBlock().writeEntry(index, entry);
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
        return nextIndex.get() - prevIndex - 1;
    }

    private void loadExistingState() {
        try {
            Predicate<String> matchPredicate = ENTRY_BLOCK_PATTERN.asMatchPredicate();
            Files.createDirectories(logDirectoryPath);
            try (final Stream<Path> directoryListing = Files.list(logDirectoryPath)) {
                directoryListing.filter(path -> matchPredicate.test(path.getFileName().toString()))
                        .sorted()
                        .map(p -> new LogBlockHolder(idSerializer, p))
                        .forEach(lbh -> logBlocks.put(lbh.blockId, lbh));
            }
            final List<LogBlockHolder> allBlocks = new ArrayList<>(logBlocks.values());
            if (allBlocks.size() > 0) {
                final LogBlock firstBlock = allBlocks.get(0).getLogBlock();
                prevTerm = firstBlock.getPrevTerm();
                prevIndex = firstBlock.getPrevIndex();
                nextIndex.set(1);
                for (int i = allBlocks.size() - 1; i >= 0; i--) {
                    final LogBlock lastBlock = allBlocks.get(i).getLogBlock();
                    final int entryCount = lastBlock.getEntryCount();
                    if (entryCount > 0) {
                        nextIndex.set(lastBlock.getPrevIndex() + entryCount + 1);
                        break;
                    }
                }
            } else {
                final LogBlockHolder firstLogBlock = new LogBlockHolder(idSerializer, logDirectoryPath.resolve(filenameForPrevIndex(0)), 0, 0, entriesPerBlock);
                firstLogBlock.getLogBlock().setPrevTerm(Term.ZERO);
                logBlocks.put(0, firstLogBlock);
                prevTerm = Term.ZERO;
                prevIndex = 0;
                nextIndex.set(1);
            }
        } catch (IOException e) {
            throw new IllegalStateException("Couldn't load existing state", e);
        }
    }

    private int blockIdForIndex(int index) {
        int zeroBasedIndex = index - 1;
        return zeroBasedIndex / entriesPerBlock;
    }

    private String filenameForPrevIndex(int prevIndex) {
        return String.format("%010x.log", prevIndex);
    }

    @Override
    public void close() {
        closeQuietly(logBlocks);
    }

    private static class LogBlockHolder implements Closeable {

        private final IDSerializer idSerializer;
        private final int blockId;
        private final Path logBlockFile;
        private LogBlock logBlock;

        public LogBlockHolder(IDSerializer idSerializer, Path logBlockFile) {
            this.idSerializer = idSerializer;
            this.logBlockFile = logBlockFile;
            this.blockId = Integer.parseInt(logBlockFile.getFileName().toString().substring(0, 10), 16);
        }

        public LogBlockHolder(IDSerializer idSerializer, Path logBlockFile, int blockId, int prevIndex, int blockLength) {
            this.idSerializer = idSerializer;
            this.logBlockFile = logBlockFile;
            this.blockId = blockId;
            try {
                logBlock = new LogBlock(idSerializer, blockLength, createRandomAccessFile(), prevIndex);
            } catch (IOException e) {
                throw new IllegalStateException("Error creating log block", e);
            }
        }

        public LogBlock getLogBlock() {
            try {
                if (logBlock == null) {
                    logBlock = new LogBlock(idSerializer, createRandomAccessFile());
                }
                return logBlock;
            } catch (IOException e) {
                throw new IllegalStateException("Couldn't load index block " + logBlockFile.toAbsolutePath());
            }
        }

        private RandomAccessFile createRandomAccessFile() throws FileNotFoundException {
            return new RandomAccessFile(logBlockFile.toFile(), "rw");
        }

        public void delete() {
            logBlock.closeWithoutForce();
            try {
                Files.deleteIfExists(logBlockFile);
            } catch (IOException e) {
                LOGGER.error("Error deleting log block file " + logBlockFile.toAbsolutePath());
            }
        }

        @Override
        public void close() throws IOException {
            closeQuietly(logBlock);
        }
    }
}
