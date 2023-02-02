package au.id.tindall.distalg.raft.log.storage;

import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.ClientRegistrationEntry;
import au.id.tindall.distalg.raft.log.entries.LogEntry;
import au.id.tindall.distalg.raft.state.InMemorySnapshot;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class PersistentLogStorageTest extends AbstractLogStorageTest<PersistentLogStorage> {
    private File tempFile;

    @Override
    protected PersistentLogStorage createLogStorage() throws IOException {
        tempFile = File.createTempFile("logFileTest", "append");
        return new PersistentLogStorage(tempFile.toPath(), 0);
    }

    @Override
    protected PersistentLogStorage createLogStorageWithTruncationBuffer(int truncationBuffer) throws IOException {
        tempFile = File.createTempFile("logFileTest", "append");
        return new PersistentLogStorage(tempFile.toPath(), truncationBuffer);
    }

    @Test
    void willAddAfterReIndex() {
        List<LogEntry> entries = List.of(
                new ClientRegistrationEntry(TERM, 123),
                new ClientRegistrationEntry(TERM, 456)
        );
        entries.forEach(entry -> storage.add(nextEntryIndex(), entry));
        storage.reIndex();
        assertThat(storage.getEntries()).usingFieldByFieldElementComparator()
                .isEqualTo(entries);

        LogEntry next = new ClientRegistrationEntry(TERM, 111);
        storage.add(nextEntryIndex(), next);
        assertThat(storage.getEntries()).usingFieldByFieldElementComparator()
                .isEqualTo(List.of(entries.get(0), entries.get(1), next));
    }

    @Test
    void willRestoreFromDiskAfterTruncate() {
        for (int i = 0; i < 100; i++) {
            storage.add(i + 1, new ClientRegistrationEntry(Term.ZERO, i + 6));
        }
        assertThat(storage.getPrevIndex()).isZero();
        assertThat(storage.getLastLogIndex()).isEqualTo(100);

        storage.installSnapshot(new InMemorySnapshot(35, Term.ZERO, null));
        assertThat(storage.getPrevIndex()).isEqualTo(35);
        assertThat(storage.getLastLogIndex()).isEqualTo(100);

        storage = new PersistentLogStorage(tempFile.toPath());
        assertThat(storage.getPrevIndex()).isEqualTo(35);
        assertThat(storage.getLastLogIndex()).isEqualTo(100);
    }

    @AfterEach
    void tearDown() {
        tempFile.delete();
    }
}