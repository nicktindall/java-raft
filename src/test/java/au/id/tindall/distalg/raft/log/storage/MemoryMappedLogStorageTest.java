package au.id.tindall.distalg.raft.log.storage;

import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.ClientRegistrationEntry;
import au.id.tindall.distalg.raft.log.entries.ConfigurationEntry;
import au.id.tindall.distalg.raft.serialisation.IntegerIDSerializer;
import au.id.tindall.distalg.raft.state.InMemorySnapshot;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.Collections;

import static org.apache.logging.log4j.LogManager.getLogger;
import static org.assertj.core.api.Assertions.assertThat;

class MemoryMappedLogStorageTest extends AbstractLogStorageTest<MemoryMappedLogStorage> {

    private static final Logger LOGGER = getLogger();

    @TempDir(cleanup = CleanupMode.ON_SUCCESS)
    Path tempDir;

    @Override
    protected MemoryMappedLogStorage createLogStorage() {
        LOGGER.info("temp directory is {}", tempDir);
        return new MemoryMappedLogStorage(IntegerIDSerializer.INSTANCE, 1, tempDir, 0);
    }

    @Override
    protected MemoryMappedLogStorage createLogStorageWithTruncationBuffer(int truncationBuffer) {
        LOGGER.info("temp directory is {}", tempDir);
        return new MemoryMappedLogStorage(IntegerIDSerializer.INSTANCE, 1, tempDir, truncationBuffer);
    }

    @ParameterizedTest
    @ValueSource(ints = {2, 3, 10, 100, 10_000})
    void worksAsExpectedForDifferentBLockSizes(int blockSize, @TempDir Path tempDir) {
        final MemoryMappedLogStorage logStorage = new MemoryMappedLogStorage(IntegerIDSerializer.INSTANCE, blockSize, tempDir, 0);
        LOGGER.info("Populating with 1000 entries");
        for (int i = 1; i <= 1000; i++) {
            logStorage.add(i, new ClientRegistrationEntry(new Term(i), i));
        }
        verifyAllEntriesArePresent(logStorage, 1, 1000);
        LOGGER.info("Truncating last 500 one at a time");
        for (int i = 1000; i >= 500; i--) {
            logStorage.truncate(i);
            verifyAllEntriesArePresent(logStorage, 1, i - 1);
        }
        LOGGER.info("Writing back tail");
        for (int i = 500; i <= 1000; i++) {
            logStorage.add(i, new ClientRegistrationEntry(new Term(i), i));
        }
        verifyAllEntriesArePresent(logStorage, 1, 1000);
        LOGGER.info("Truncate first 500 one at a time");
        for (int i = 1; i <= 500; i++) {
            logStorage.installSnapshot(new InMemorySnapshot(i, Term.ZERO, new ConfigurationEntry(Term.ZERO, Collections.emptySet())));
            verifyAllEntriesArePresent(logStorage, i + 1, 600);
        }
        LOGGER.info("Truncate 501 -> 800 ten at a time");
        for (int i = 501; i <= 800; i = i + 10) {
            logStorage.installSnapshot(new InMemorySnapshot(i, Term.ZERO, new ConfigurationEntry(Term.ZERO, Collections.emptySet())));
            verifyAllEntriesArePresent(logStorage, i + 1, 1_000);
        }
    }

    @Test
    void willRestoreFromDisk() {
        for (int i = 1; i <= 100; i++) {
            storage.add(i, new ClientRegistrationEntry(new Term(i), i));
        }
        assertThat(storage.getPrevIndex()).isZero();
        assertThat(storage.getLastLogIndex()).isEqualTo(100);

        storage.close();
        storage = new MemoryMappedLogStorage(IntegerIDSerializer.INSTANCE, 1, tempDir, 1);
        assertThat(storage.getPrevIndex()).isZero();
        assertThat(storage.getLastLogIndex()).isEqualTo(100);

        for (int i = 101; i <= 110; i++) {
            storage.add(i, new ClientRegistrationEntry(new Term(i), i));
        }
        assertThat(storage.getPrevIndex()).isZero();
        assertThat(storage.getLastLogIndex()).isEqualTo(110);

        verifyAllEntriesArePresent(storage, 1, 110);
    }

    @Test
    void willRestoreFromDiskAfterTruncate() {
        for (int i = 1; i <= 100; i++) {
            storage.add(i, new ClientRegistrationEntry(new Term(i), i));
        }
        assertThat(storage.getPrevIndex()).isZero();
        assertThat(storage.getLastLogIndex()).isEqualTo(100);

        storage.truncate(76);
        assertThat(storage.getPrevIndex()).isEqualTo(0);
        assertThat(storage.getLastLogIndex()).isEqualTo(75);

        storage.close();
        storage = new MemoryMappedLogStorage(IntegerIDSerializer.INSTANCE, 1, tempDir, 1);
        assertThat(storage.getPrevIndex()).isEqualTo(0);
        assertThat(storage.getLastLogIndex()).isEqualTo(75);

        verifyAllEntriesArePresent(storage, 1, 75);
    }

    @Test
    void willRestoreFromDiskAfterSnapshot() {
        for (int i = 1; i <= 100; i++) {
            storage.add(i, new ClientRegistrationEntry(new Term(i), i));
        }
        assertThat(storage.getPrevIndex()).isZero();
        assertThat(storage.getLastLogIndex()).isEqualTo(100);

        storage.installSnapshot(new InMemorySnapshot(35, Term.ZERO, null));
        assertThat(storage.getPrevIndex()).isEqualTo(35);
        assertThat(storage.getLastLogIndex()).isEqualTo(100);

        storage.close();
        storage = new MemoryMappedLogStorage(IntegerIDSerializer.INSTANCE, 1, tempDir, 1);
        assertThat(storage.getPrevIndex()).isEqualTo(35);
        assertThat(storage.getLastLogIndex()).isEqualTo(100);

        for (int i = 101; i <= 110; i++) {
            storage.add(i, new ClientRegistrationEntry(new Term(i), i));
        }
        assertThat(storage.getPrevIndex()).isEqualTo(35);
        assertThat(storage.getLastLogIndex()).isEqualTo(110);

        verifyAllEntriesArePresent(storage, 36, 110);
    }

    private void verifyAllEntriesArePresent(MemoryMappedLogStorage logStorage, int firstIndexInclusive, int lastIndexInclusive) {
        for (int i = firstIndexInclusive; i <= lastIndexInclusive; i++) {
            try {
                final ClientRegistrationEntry entry = (ClientRegistrationEntry) logStorage.getEntry(i);
                assertThat(entry.getTerm().getNumber()).isEqualTo(i);
                assertThat(entry.getClientId()).isEqualTo(i);
            } catch (Exception e) {
                LOGGER.error("Failed to read entry {}", i);
                throw e;
            }
        }
    }
}