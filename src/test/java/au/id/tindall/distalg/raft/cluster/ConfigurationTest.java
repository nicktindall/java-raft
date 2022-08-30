package au.id.tindall.distalg.raft.cluster;

import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.ConfigurationEntry;
import au.id.tindall.distalg.raft.state.InMemorySnapshot;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Set;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class ConfigurationTest {

    public static final int SERVER_ID = 123;
    private Configuration<Integer> configuration;

    @BeforeEach
    void setUp() {
        configuration = new Configuration<>(SERVER_ID, Set.of(45, 67, 89), Duration.ofMillis(5_000));
    }

    @Test
    void willUpdateClusterMembersWhenConfigEntryAppended() {
        configuration.entryAppended(5, new ConfigurationEntry(Term.ZERO, Set.of(10, 11)));
        assertThat(configuration.getServers()).isEqualTo(Set.of(10, 11));

        configuration.entryAppended(6, new ConfigurationEntry(Term.ZERO, Set.of(12, 13)));
        assertThat(configuration.getServers()).isEqualTo(Set.of(12, 13));
    }

    @Nested
    class OnSnapshotInstalled {

        @Test
        void willUpdateClusterMembersWhenSnapshotConfigIsMoreUpToDate_SameTermHigherIndex() {
            configuration.onSnapshotInstalled(new InMemorySnapshot(300, Term.ZERO, new ConfigurationEntry(Term.ZERO, Set.of(10, 11))));
            assertThat(configuration.getServers()).isEqualTo(Set.of(10, 11));


            configuration.onSnapshotInstalled(new InMemorySnapshot(320, Term.ZERO, new ConfigurationEntry(Term.ZERO, Set.of(12, 13))));
            assertThat(configuration.getServers()).isEqualTo(Set.of(12, 13));
        }

        @Test
        void willUpdateClusterMembersWhenSnapshotConfigIsMoreUpToDate_HigherTermLowerIndex() {
            configuration.onSnapshotInstalled(new InMemorySnapshot(300, Term.ZERO, new ConfigurationEntry(Term.ZERO, Set.of(10, 11))));
            assertThat(configuration.getServers()).isEqualTo(Set.of(10, 11));


            configuration.onSnapshotInstalled(new InMemorySnapshot(290, new Term(1), new ConfigurationEntry(Term.ZERO, Set.of(12, 13))));
            assertThat(configuration.getServers()).isEqualTo(Set.of(12, 13));
        }

        @Test
        void willNotUpdateClusterMembersWhenSnapshotConfigIsLessUpToDate_SameTermLowerIndex() {
            configuration.onSnapshotInstalled(new InMemorySnapshot(300, Term.ZERO, new ConfigurationEntry(Term.ZERO, Set.of(10, 11))));
            assertThat(configuration.getServers()).isEqualTo(Set.of(10, 11));


            configuration.onSnapshotInstalled(new InMemorySnapshot(290, Term.ZERO, new ConfigurationEntry(Term.ZERO, Set.of(12, 13))));
            assertThat(configuration.getServers()).isEqualTo(Set.of(10, 11));
        }
    }
}