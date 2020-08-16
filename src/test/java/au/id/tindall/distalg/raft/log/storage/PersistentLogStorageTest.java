package au.id.tindall.distalg.raft.log.storage;

import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.ClientRegistrationEntry;
import au.id.tindall.distalg.raft.log.entries.LogEntry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class PersistentLogStorageTest {

    public static final Term TERM = new Term(0);

    private File tempFile;
    private PersistentLogStorage storage;

    @BeforeEach
    void setUp() throws IOException {
        tempFile = File.createTempFile("logFileTest", "append");
        storage = new PersistentLogStorage(tempFile.toPath());
    }

    @AfterEach
    void tearDown() {
        tempFile.delete();
    }

    @Test
    public void willAdd() {
        List<LogEntry> entries = List.of(
                new ClientRegistrationEntry(TERM, 123),
                new ClientRegistrationEntry(TERM, 456),
                new ClientRegistrationEntry(TERM, 789)
        );
        entries.forEach(storage::add);
        assertThat(storage.getEntries()).usingFieldByFieldElementComparator()
                .isEqualTo(entries);
    }

    @Test
    public void willTruncate() {
        List<LogEntry> entries = List.of(
                new ClientRegistrationEntry(TERM, 123),
                new ClientRegistrationEntry(TERM, 456),
                new ClientRegistrationEntry(TERM, 789)
        );
        entries.forEach(storage::add);
        storage.truncate(2);
        ClientRegistrationEntry next = new ClientRegistrationEntry(TERM, 111);
        storage.add(next);
        assertThat(storage.getEntries()).usingFieldByFieldElementComparator()
                .isEqualTo(List.of(entries.get(0), next));
    }

    @Test
    public void willTruncateToZero() {
        List<LogEntry> entries = List.of(
                new ClientRegistrationEntry(TERM, 123),
                new ClientRegistrationEntry(TERM, 456),
                new ClientRegistrationEntry(TERM, 789)
        );
        entries.forEach(storage::add);
        storage.truncate(1);
        ClientRegistrationEntry next = new ClientRegistrationEntry(TERM, 111);
        storage.add(next);
        assertThat(storage.getEntries()).usingFieldByFieldElementComparator()
                .isEqualTo(List.of(next));
    }

    @Test
    public void willAddAfterReIndex() {
        List<LogEntry> entries = List.of(
                new ClientRegistrationEntry(TERM, 123),
                new ClientRegistrationEntry(TERM, 456)
        );
        entries.forEach(storage::add);
        storage.reIndex();
        assertThat(storage.getEntries()).usingFieldByFieldElementComparator()
                .isEqualTo(entries);

        LogEntry next = new ClientRegistrationEntry(TERM, 111);
        storage.add(next);
        assertThat(storage.getEntries()).usingFieldByFieldElementComparator()
                .isEqualTo(List.of(entries.get(0), entries.get(1), next));
    }

    @Test
    void willGetEntry() {
        List<LogEntry> entries = List.of(
                new ClientRegistrationEntry(TERM, 123),
                new ClientRegistrationEntry(TERM, 456)
        );
        entries.forEach(storage::add);
        for (int i = 0; i < 2; i++) {
            assertThat(storage.getEntry(i + 1)).usingRecursiveComparison().isEqualTo(entries.get(i));
        }
    }

    @Test
    void willGetAllEntries() {
        List<LogEntry> entries = List.of(
                new ClientRegistrationEntry(TERM, 123),
                new ClientRegistrationEntry(TERM, 456),
                new ClientRegistrationEntry(TERM, 789)
        );
        entries.forEach(storage::add);
        assertThat(storage.getEntries()).usingFieldByFieldElementComparator()
                .isEqualTo(entries);
    }

    @Test
    void willGetEntries() {
        List<LogEntry> entries = List.of(
                new ClientRegistrationEntry(TERM, 123),
                new ClientRegistrationEntry(TERM, 456),
                new ClientRegistrationEntry(TERM, 789)

        );
        entries.forEach(storage::add);
        assertThat(storage.getEntries(1, 3)).usingFieldByFieldElementComparator()
                .isEqualTo(entries.subList(0, 2));
    }
}