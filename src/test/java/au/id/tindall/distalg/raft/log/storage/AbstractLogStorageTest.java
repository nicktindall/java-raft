package au.id.tindall.distalg.raft.log.storage;

import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.ClientRegistrationEntry;
import au.id.tindall.distalg.raft.log.entries.ConfigurationEntry;
import au.id.tindall.distalg.raft.log.entries.LogEntry;
import au.id.tindall.distalg.raft.state.InMemorySnapshot;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static au.id.tindall.distalg.raft.log.EntryStatus.AfterEnd;
import static au.id.tindall.distalg.raft.log.EntryStatus.BeforeStart;
import static au.id.tindall.distalg.raft.log.EntryStatus.Present;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

abstract class AbstractLogStorageTest<L extends LogStorage> {

    public static final Term TERM = new Term(0);
    private final AtomicInteger entryIndex = new AtomicInteger(1);

    protected L storage;

    @BeforeEach
    void setUp() throws IOException {
        entryIndex.set(1);
        storage = createLogStorage();
    }

    protected abstract L createLogStorage() throws IOException;

    protected abstract L createLogStorageWithTruncationBuffer(int truncationBuffer) throws IOException;

    protected int nextEntryIndex() {
        return entryIndex.getAndIncrement();
    }

    @Nested
    class Add {

        @Test
        public void willAddWhenIndexMatches() {
            List<LogEntry> entries = List.of(
                    new ClientRegistrationEntry(TERM, 123),
                    new ClientRegistrationEntry(TERM, 456),
                    new ClientRegistrationEntry(TERM, 789)
            );
            entries.forEach(entry -> storage.add(nextEntryIndex(), entry));
            assertThat(storage.getEntries()).usingFieldByFieldElementComparator()
                    .isEqualTo(entries);
        }

        @Test
        void willThrowWhenIndexDoesNotMatch() {
            assertThrows(IllegalArgumentException.class, () -> {
                storage.add(2, new ClientRegistrationEntry(TERM, 123));
            });
        }
    }

    @Test
    public void willTruncate() {
        List<LogEntry> entries = List.of(
                new ClientRegistrationEntry(TERM, 123),
                new ClientRegistrationEntry(TERM, 456),
                new ClientRegistrationEntry(TERM, 789)
        );
        entries.forEach(entry -> storage.add(nextEntryIndex(), entry));
        storage.truncate(2);
        ClientRegistrationEntry next = new ClientRegistrationEntry(TERM, 111);
        storage.add(2, next);
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
        entries.forEach(entry -> storage.add(nextEntryIndex(), entry));
        storage.truncate(1);
        ClientRegistrationEntry next = new ClientRegistrationEntry(TERM, 111);
        storage.add(1, next);
        assertThat(storage.getEntries()).usingFieldByFieldElementComparator()
                .isEqualTo(List.of(next));
    }

    @Test
    void willGetEntry() {
        List<LogEntry> entries = List.of(
                new ClientRegistrationEntry(TERM, 123),
                new ClientRegistrationEntry(TERM, 456)
        );
        entries.forEach(entry -> storage.add(nextEntryIndex(), entry));
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
        entries.forEach(entry -> storage.add(nextEntryIndex(), entry));
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
        entries.forEach(entry -> storage.add(nextEntryIndex(), entry));
        assertThat(storage.getEntries(1, 3)).usingFieldByFieldElementComparator()
                .isEqualTo(entries.subList(0, 2));
    }

    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    @Nested
    class InstallSnapshot {

        List<LogEntry> entries = List.of(
                new ClientRegistrationEntry(TERM, 123),
                new ClientRegistrationEntry(TERM, 456),
                new ClientRegistrationEntry(TERM, 789),
                new ClientRegistrationEntry(TERM, 987),
                new ClientRegistrationEntry(TERM, 876)
        );

        Stream<Arguments> params() {
            return IntStream.range(1, 6)
                    .mapToObj(i -> Arguments.of(Named.of("Truncated up to " + i, i)));
        }

        void populateThenSnapshot(int lastIndex) {
            entries.forEach(entry -> storage.add(nextEntryIndex(), entry));
            storage.installSnapshot(new InMemorySnapshot(lastIndex, TERM, new ConfigurationEntry(TERM, Set.of(1, 2, 3))));
        }

        @Test
        void willReturnCorrectEntriesAfterWholeLogIsSnapshotted() {
            populateThenSnapshot(10);
            final ClientRegistrationEntry entry = new ClientRegistrationEntry(TERM, 99999);
            storage.add(11, entry);
            assertThat(storage.getEntries()).usingFieldByFieldElementComparator().isEqualTo(List.of(entry));
        }

        @ParameterizedTest
        @MethodSource("params")
        void willReturnCorrectEntriesIndividuallyAfterLogIsSnapshotted(int lastIndex) {
            populateThenSnapshot(lastIndex);
            IntStream.range(1, lastIndex + 5).forEach(i -> {
                if (i <= lastIndex) {
                    assertThatThrownBy(() -> storage.getEntry(i))
                            .isInstanceOf(IndexOutOfBoundsException.class)
                            .hasMessage(String.format("Index has been truncated by log compaction (%d <= %d)", i, lastIndex));
                } else if (i > 5) {
                    assertThatThrownBy(() -> storage.getEntry(i))
                            .isInstanceOf(IndexOutOfBoundsException.class)
                            .hasMessage(String.format("Index is after end of log (%d > 5)", i));
                } else {
                    assertThat(storage.getEntry(i)).usingRecursiveComparison().isEqualTo(entries.get(i - 1));
                }
            });
        }

        @Test
        void willReturnCorrectEntriesAfterWholeLogIsSnapshottedExact() {
            populateThenSnapshot(5);
            final ClientRegistrationEntry entry = new ClientRegistrationEntry(TERM, 99999);
            storage.add(6, entry);
            assertThat(storage.getEntries()).usingFieldByFieldElementComparator().isEqualTo(List.of(entry));
        }

        @ParameterizedTest
        @MethodSource("params")
        void willCorrectlyReportSize(int lastIndex) {
            populateThenSnapshot(lastIndex);
            assertEquals(5 - lastIndex, storage.size());
        }

        @ParameterizedTest
        @MethodSource("params")
        void willCorrectlyReportFirstLogIndex(int lastIndex) {
            populateThenSnapshot(lastIndex);
            assertEquals(lastIndex + 1, storage.getFirstLogIndex());
        }

        @ParameterizedTest
        @MethodSource("params")
        void willCorrectlyReportLastLogIndex(int lastIndex) {
            populateThenSnapshot(lastIndex);
            assertEquals(5, storage.getLastLogIndex());
        }

        @ParameterizedTest
        @MethodSource("params")
        void willCorrectlyReportPrevIndex(int lastIndex) {
            populateThenSnapshot(lastIndex);
            assertThat(storage.getPrevIndex()).isEqualTo(lastIndex);
        }

        @ParameterizedTest
        @MethodSource("params")
        void willCorrectlyReturnAllEntries(int lastIndex) {
            populateThenSnapshot(lastIndex);
            assertThat(storage.getEntries()).usingFieldByFieldElementComparator()
                    .isEqualTo(entries.subList(lastIndex, entries.size()));
        }

        @ParameterizedTest
        @MethodSource("params")
        void willCorrectlyIdentifyIfAnIndexIsPresent(int lastIndex) {
            populateThenSnapshot(lastIndex);
            for (int i = 1; i < lastIndex + 5; i++) {
                assertThat(storage.hasEntry(i)).isEqualTo(i <= lastIndex ? BeforeStart : i > 5 ? AfterEnd : Present);
            }
        }

        @ParameterizedTest
        @MethodSource("params")
        void willLeaveTruncationBufferEntriesWhenConfigured(int lastIndex) throws IOException {
            final int truncationBufferSize = 3;
            storage = createLogStorageWithTruncationBuffer(truncationBufferSize);
            populateThenSnapshot(lastIndex);
            assertThat(storage.getEntries()).usingFieldByFieldElementComparator()
                    .isEqualTo(entries.subList(Math.max(lastIndex - truncationBufferSize, 0), entries.size()));
            assertThat(storage.size())
                    .isEqualTo(Math.min(5, 5 - lastIndex + truncationBufferSize));
            assertThat(storage.getPrevIndex())
                    .isEqualTo(Math.max(lastIndex - truncationBufferSize, 0));
        }
    }
}