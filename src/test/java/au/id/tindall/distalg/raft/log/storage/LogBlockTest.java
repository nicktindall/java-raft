package au.id.tindall.distalg.raft.log.storage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LogBlockTest {

    private File logBlockFile;
    private LogBlock logBlock;

    @BeforeEach
    void setUp(@TempDir Path tempDir) throws IOException {
        this.logBlockFile = tempDir.resolve("test.block").toFile();
        logBlock = new LogBlock(100, new RandomAccessFile(logBlockFile, "rw"), 0);
    }

    @Test
    void canWriteAndReadEntry() {
        final byte[] bytes = "testing one two three".getBytes();
        logBlock.writeEntry(1, ByteBuffer.wrap(bytes));
        final ByteBuffer readBuffer = ByteBuffer.allocate(100);
        logBlock.readEntry(1, readBuffer);
        assertThat(readBuffer.flip()).isEqualByComparingTo(ByteBuffer.wrap(bytes));
    }

    @Test
    void canReadAndWriteOneHundredEntries() {
        final ByteBuffer readBuffer = ByteBuffer.allocate(100);
        for (int i = 1; i <= 100; i++) {
            final byte[] bytes = ("************************************************* testing one two three " + i).getBytes();
            logBlock.writeEntry(i, ByteBuffer.wrap(bytes));
            logBlock.readEntry(i, readBuffer.clear());
            assertThat(readBuffer.flip()).isEqualByComparingTo(ByteBuffer.wrap(bytes));
        }
    }

    @Test
    void willThrowWhenWriteEntryCalledWithNotNextIndex() {
        assertThatThrownBy(() -> logBlock.writeEntry(2, ByteBuffer.wrap("will throw".getBytes())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Attempted to write 2, next expected index is 1");
    }

    @Test
    void willThrowWhenReadEntryCalledWithNonExistentIndex() {
        assertThatThrownBy(() -> logBlock.readEntry(1, ByteBuffer.wrap("will throw".getBytes())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Attempted to read index 1, file is empty");
    }

    @Test
    void canTruncateThenWriteMore() {
        for (int i = 1; i <= 50; i++) {
            logBlock.writeEntry(i, bufferContaining(i));
        }
        final ByteBuffer readBuffer = ByteBuffer.allocate(100);
        logBlock.truncate(34);
        for (int i = 1; i < 34; i++) {
            logBlock.readEntry(i, readBuffer.clear());
            assertThat(readBuffer.flip()).isEqualTo(bufferContaining(i));
        }
        assertThatThrownBy(() -> logBlock.readEntry(34, readBuffer.clear()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Attempted to read index 34, file contains indices 1 to 33");
        for (int i = 34; i <= 50; i++) {
            logBlock.writeEntry(i, bufferContaining(i));
        }
        for (int i = 1; i <= 50; i++) {
            logBlock.readEntry(i, readBuffer.clear());
            assertThat(readBuffer.flip()).isEqualTo(bufferContaining(i));
        }
    }

    @Test
    void canTruncateAllEntries(@TempDir Path tempDir) throws IOException {
        LogBlock blockStartingAt100 = new LogBlock(100, new RandomAccessFile(tempDir.resolve("100prev").toFile(), "rw"), 100);
        for (int i = 101; i <= 111; i++) {
            blockStartingAt100.writeEntry(i, bufferContaining(i));
        }
        blockStartingAt100.truncate(85);
        blockStartingAt100.writeEntry(101, bufferContaining(123));
    }

    @Test
    void willThrowWhenReadBufferIsTooSmall() {
        logBlock.writeEntry(1, ByteBuffer.wrap("a really long string".getBytes()));
        assertThatThrownBy(() -> logBlock.readEntry(1, ByteBuffer.allocate(5)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Can't read entry 1, length is 20, provided buffer has 5 remaining capacity");
    }

    @Test
    void canReadAndWriteAfterRestore() throws IOException {
        for (int i = 1; i <= 30; i++) {
            logBlock.writeEntry(i, bufferContaining(i));
        }
        logBlock.close();
        logBlock = new LogBlock(new RandomAccessFile(logBlockFile, "rw"));
        for (int i = 31; i <= 35; i++) {
            logBlock.writeEntry(i, bufferContaining(i));
        }
        ByteBuffer readBuffer = ByteBuffer.allocate(64);
        for (int i = 1; i <= 35; i++) {
            logBlock.readEntry(i, readBuffer.clear());
            assertThat(readBuffer.flip()).isEqualTo(bufferContaining(i));
        }
    }

    @Test
    void canReadAndWriteAfterRestore_empty() throws IOException {
        logBlock.close();
        logBlock = new LogBlock(new RandomAccessFile(logBlockFile, "rw"));
        for (int i = 1; i <= 30; i++) {
            logBlock.writeEntry(i, bufferContaining(i));
        }
        ByteBuffer readBuffer = ByteBuffer.allocate(64);
        for (int i = 1; i <= 30; i++) {
            logBlock.readEntry(i, readBuffer.clear());
            assertThat(readBuffer.flip()).isEqualTo(bufferContaining(i));
        }
    }

    private ByteBuffer bufferContaining(int i) {
        return ByteBuffer.wrap(String.valueOf(i).getBytes());
    }
}