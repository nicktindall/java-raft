package au.id.tindall.distalg.raft.log.storage;

import au.id.tindall.distalg.raft.log.entries.ClientRegistrationEntry;
import au.id.tindall.distalg.raft.log.entries.LogEntry;
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
        return new PersistentLogStorage(tempFile.toPath());
    }

    @Test
    public void willAddAfterReIndex() {
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


    @AfterEach
    void tearDown() {
        tempFile.delete();
    }
}