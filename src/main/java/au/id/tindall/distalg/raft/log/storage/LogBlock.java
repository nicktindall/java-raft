package au.id.tindall.distalg.raft.log.storage;

import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.util.BufferUtil;
import au.id.tindall.distalg.raft.util.Closeables;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static java.lang.String.format;
import static org.apache.logging.log4j.LogManager.getLogger;

/**
 * Structure is
 * <p>
 * 0: prevIndex
 * 4: prevTerm
 * 8: entryCount
 * 12 -> (8 + MAX_ENTRIES * Integer.BYTES): indices
 * *: entries
 */
public class LogBlock implements Closeable {

    private static final Logger LOGGER = getLogger();

    private static final int MAX_ENTRIES_INDEX = 0;
    private static final int PREV_INDEX_INDEX = MAX_ENTRIES_INDEX + Integer.BYTES;
    private static final int PREV_TERM_INDEX = PREV_INDEX_INDEX + Integer.BYTES;
    private static final int ENTRY_COUNT_INDEX = PREV_TERM_INDEX + Integer.BYTES;
    private static final int START_OF_INDEX = ENTRY_COUNT_INDEX + Integer.BYTES;
    private static final int EOF = Integer.MIN_VALUE;
    private static final int ESTIMATED_MAX_ENTRY_SIZE = 10_240; // 10KB

    private final RandomAccessFile blockRandomAccessFile;
    private int maxEntries;
    private int prevIndex;
    private Term prevTerm;
    private MappedByteBuffer mbb;
    private int nextIndexToWrite;
    private int nextIndexPosition;
    private int nextPositionToWrite;

    /**
     * Constructor for creating a new block
     *
     * @param maxEntries
     * @param blockRandomAccessFile
     * @param prevIndex
     */
    public LogBlock(int maxEntries, RandomAccessFile blockRandomAccessFile, int prevIndex) throws IOException {
        this.maxEntries = maxEntries;
        this.blockRandomAccessFile = blockRandomAccessFile;
        this.prevIndex = prevIndex;
        initialiseState();
    }

    /**
     * Constructor for loading an existing block
     *
     * @param blockRandomAccessFile
     * @throws IOException
     */
    public LogBlock(RandomAccessFile blockRandomAccessFile) throws IOException {
        this.blockRandomAccessFile = blockRandomAccessFile;
        loadExistingState();
    }

    private void loadExistingState() throws IOException {
        this.blockRandomAccessFile.seek(MAX_ENTRIES_INDEX);
        this.maxEntries = this.blockRandomAccessFile.readInt();
        final int totalSize = START_OF_INDEX + indexSize() + (maxEntries * ESTIMATED_MAX_ENTRY_SIZE);
        this.mbb = this.blockRandomAccessFile.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, totalSize);
        if (this.maxEntries != mbb.getInt(MAX_ENTRIES_INDEX)) {
            throw new IllegalArgumentException(format("Persisted maxEntries (%,d) does not match specified maxEntries (%,d)", mbb.getInt(MAX_ENTRIES_INDEX), maxEntries));
        }
        this.prevIndex = mbb.getInt(PREV_INDEX_INDEX);
        this.prevTerm = new Term(mbb.getInt(PREV_TERM_INDEX));
        final int entryCount = getEntryCount();
        this.nextIndexPosition = START_OF_INDEX + (entryCount * Integer.BYTES);
        this.nextIndexToWrite = prevIndex + entryCount + 1;
        this.nextPositionToWrite = entryCount == 0 ?
                START_OF_INDEX + indexSize()
                : endPositionOfEntry(indexInFile(nextIndexToWrite - 1));
    }

    private int endPositionOfEntry(int indexInFile) {
        final int startPositionOfEntry = startPositionOfEntry(indexInFile);
        return startPositionOfEntry + mbb.getInt(startPositionOfEntry) + Integer.BYTES;
    }

    private int indexSize() {
        return (maxEntries + 1) * Integer.BYTES;
    }

    private void initialiseState() throws IOException {
        final int totalSize = START_OF_INDEX + indexSize() + (maxEntries * ESTIMATED_MAX_ENTRY_SIZE);
        LOGGER.debug("Initialising block to {}MB", totalSize / (1024 * 1024));
        this.mbb = this.blockRandomAccessFile.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, totalSize);
        this.mbb.putInt(MAX_ENTRIES_INDEX, maxEntries);
        this.mbb.putInt(PREV_INDEX_INDEX, prevIndex);
        this.mbb.putInt(ENTRY_COUNT_INDEX, 0);
        this.nextIndexPosition = START_OF_INDEX;
        this.mbb.putInt(this.nextIndexPosition, EOF);
        this.nextPositionToWrite = START_OF_INDEX + indexSize();
        this.nextIndexToWrite = prevIndex + 1;
    }

    public void setPrevTerm(Term term) {
        this.prevTerm = term;
        this.mbb.putInt(PREV_TERM_INDEX, term.getNumber());
    }

    public void writeEntry(int index, ByteBuffer buffer) {
        if (getEntryCount() == maxEntries) {
            throw new IllegalStateException("Entry block is full (already contains " + maxEntries + " entries)");
        }
        if (nextIndexToWrite == index) {
            incrementEntryCount();
            nextIndexToWrite++;
            mbb.putInt(nextIndexPosition, nextPositionToWrite);
            nextIndexPosition = nextIndexPosition + Integer.BYTES;
            mbb.putInt(nextIndexPosition, EOF);
            final int entryLength = buffer.remaining();
            mbb.position(nextPositionToWrite).putInt(entryLength).put(buffer);
            nextPositionToWrite = mbb.position();
        } else {
            throw new IllegalArgumentException(format("Attempted to write %,d, next expected index is %,d", index, nextIndexToWrite));
        }
    }

    public void readEntry(int index, ByteBuffer buffer) {
        final int indexInFile = indexInFile(index);
        final int entryCount = getEntryCount();
        if (indexInFile < entryCount) {
            readEntry(index, indexInFile, buffer);
        } else {
            String containsString = entryCount == 0 ? "is empty" : format("contains indices %,d to %,d", prevIndex + 1, prevIndex + entryCount);
            throw new IllegalArgumentException(format("Attempted to read index %,d, file ", index) + containsString);
        }
    }

    private int indexInFile(int index) {
        return index - prevIndex - 1;
    }

    private void readEntry(int index, int indexInFile, ByteBuffer buffer) {
        int startPosition = startPositionOfEntry(indexInFile);
        int lengthOfEntry = mbb.getInt(startPosition);
        if (buffer.remaining() < lengthOfEntry) {
            throw new IllegalArgumentException(format("Can't read entry %,d, length is %,d, provided buffer has %,d remaining capacity",
                    index, lengthOfEntry, buffer.remaining()));
        }
        final int firstByte = startPosition + Integer.BYTES;
        final int lastByte = startPosition + Integer.BYTES + lengthOfEntry;
        for (int i = firstByte; i < lastByte; i++) {
            buffer.put(mbb.get(i));
        }
    }

    public void truncate(int fromIndex) {
        if (getEntryCount() == 0) {
            return;
        }
        int localTruncateFrom = Math.max(prevIndex + 1, fromIndex);
        final int indexInFile = indexInFile(localTruncateFrom);
        setEntryCount(indexInFile);
        nextIndexToWrite = localTruncateFrom;
        nextIndexPosition = START_OF_INDEX + (indexInFile * Integer.BYTES);
        nextPositionToWrite = startPositionOfEntry(indexInFile);
        mbb.putInt(nextIndexPosition, EOF);
    }

    public int getPrevIndex() {
        return prevIndex;
    }

    public Term getPrevTerm() {
        return prevTerm;
    }

    private void incrementEntryCount() {
        setEntryCount(getEntryCount() + 1);
    }

    public int getEntryCount() {
        return mbb.getInt(ENTRY_COUNT_INDEX);
    }

    private void setEntryCount(int newValue) {
        mbb.putInt(ENTRY_COUNT_INDEX, newValue);
    }

    private int startPositionOfEntry(int indexInFile) {
        return mbb.getInt(START_OF_INDEX + (indexInFile * Integer.BYTES));
    }

    public void closeWithoutForce() {
        Closeables.closeQuietly(mbb, blockRandomAccessFile);
        BufferUtil.free(mbb);
    }

    @Override
    public void close() {
        if (mbb != null) {
            mbb.force();
        }
        closeWithoutForce();
    }
}
