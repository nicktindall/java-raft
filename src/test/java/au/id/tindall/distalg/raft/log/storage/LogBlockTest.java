package au.id.tindall.distalg.raft.log.storage;

import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.ClientRegistrationEntry;
import au.id.tindall.distalg.raft.log.entries.LogEntry;
import au.id.tindall.distalg.raft.log.entries.StateMachineCommandEntry;
import au.id.tindall.distalg.raft.serialisation.IntegerIDSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;

import static au.id.tindall.distalg.raft.util.RandomTestUtil.randomByteArray;
import static au.id.tindall.distalg.raft.util.RandomTestUtil.randomIntValue;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LogBlockTest {

    private File logBlockFile;
    private LogBlock logBlock;

    @BeforeEach
    void setUp(@TempDir Path tempDir) throws IOException {
        this.logBlockFile = tempDir.resolve("test.block").toFile();
        logBlock = new LogBlock(IntegerIDSerializer.INSTANCE, 100, new RandomAccessFile(logBlockFile, "rw"), 0);
    }

    @Test
    void canWriteAndReadEntry() {
        final StateMachineCommandEntry entry = randomEntry();
        logBlock.writeEntry(1, entry);
        assertThat(logBlock.readEntry(1)).usingRecursiveComparison().isEqualTo(entry);
    }

    @Test
    void canReadAndWriteOneHundredEntries() {
        for (int i = 1; i <= 100; i++) {
            final StateMachineCommandEntry entry = randomEntry();
            logBlock.writeEntry(i, entry);
            assertThat(logBlock.readEntry(i)).usingRecursiveComparison().isEqualTo(entry);
        }
    }

    @Test
    void willThrowWhenWriteEntryCalledWithNotNextIndex() {
        assertThatThrownBy(() -> logBlock.writeEntry(2, randomEntry()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Attempted to write 2, next expected index is 1");
    }

    @Test
    void willThrowWhenReadEntryCalledWithNonExistentIndex() {
        assertThatThrownBy(() -> logBlock.readEntry(1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Attempted to read index 1, file is empty");
    }

    @Test
    void canTruncateThenWriteMore() {
        for (int i = 1; i <= 50; i++) {
            logBlock.writeEntry(i, deterministicEntryForIndex(i));
        }
        logBlock.truncate(34);
        for (int i = 1; i < 34; i++) {
            LogEntry entry = logBlock.readEntry(i);
            assertThat(entry).usingRecursiveComparison().isEqualTo(deterministicEntryForIndex(i));
        }
        assertThatThrownBy(() -> logBlock.readEntry(34))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Attempted to read index 34, file contains indices 1 to 33");
        for (int i = 34; i <= 50; i++) {
            logBlock.writeEntry(i, deterministicEntryForIndex(i));
        }
        for (int i = 1; i <= 50; i++) {
            assertThat(logBlock.readEntry(i)).usingRecursiveComparison().isEqualTo(deterministicEntryForIndex(i));
        }
    }

    @Test
    void canTruncateAllEntries(@TempDir Path tempDir) throws IOException {
        LogBlock blockStartingAt100 = new LogBlock(IntegerIDSerializer.INSTANCE, 100, new RandomAccessFile(tempDir.resolve("100prev").toFile(), "rw"), 100);
        for (int i = 101; i <= 111; i++) {
            blockStartingAt100.writeEntry(i, randomEntry());
        }
        blockStartingAt100.truncate(85);
        blockStartingAt100.writeEntry(101, randomEntry());
    }

    @Test
    void canReadAndWriteAfterRestore() throws IOException {
        for (int i = 1; i <= 30; i++) {
            logBlock.writeEntry(i, deterministicEntryForIndex(i));
        }
        logBlock.close();
        logBlock = new LogBlock(IntegerIDSerializer.INSTANCE, new RandomAccessFile(logBlockFile, "rw"));
        for (int i = 31; i <= 35; i++) {
            logBlock.writeEntry(i, deterministicEntryForIndex(i));
        }
        for (int i = 1; i <= 35; i++) {
            assertThat(logBlock.readEntry(i)).usingRecursiveComparison().isEqualTo(deterministicEntryForIndex(i));
        }
    }

    @Test
    void canReadAndWriteAfterRestore_empty() throws IOException {
        logBlock.close();
        logBlock = new LogBlock(IntegerIDSerializer.INSTANCE, new RandomAccessFile(logBlockFile, "rw"));
        for (int i = 1; i <= 30; i++) {
            logBlock.writeEntry(i, deterministicEntryForIndex(i));
        }
        for (int i = 1; i <= 30; i++) {
            assertThat(logBlock.readEntry(i)).usingRecursiveComparison().isEqualTo(deterministicEntryForIndex(i));
        }
    }

    private static LogEntry deterministicEntryForIndex(int index) {
        return new ClientRegistrationEntry(new Term(index), index);
    }

    private static StateMachineCommandEntry randomEntry() {
        return new StateMachineCommandEntry(new Term(randomIntValue()), randomIntValue(), randomIntValue(), randomIntValue(), randomByteArray());
    }
}