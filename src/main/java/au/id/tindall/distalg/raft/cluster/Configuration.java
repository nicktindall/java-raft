package au.id.tindall.distalg.raft.cluster;

import au.id.tindall.distalg.raft.log.EntryAppendedEventHandler;
import au.id.tindall.distalg.raft.log.LogSummary;
import au.id.tindall.distalg.raft.log.entries.ConfigurationEntry;
import au.id.tindall.distalg.raft.log.entries.LogEntry;
import au.id.tindall.distalg.raft.state.Snapshot;
import au.id.tindall.distalg.raft.state.SnapshotInstalledListener;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class Configuration<I> implements EntryAppendedEventHandler, SnapshotInstalledListener {

    private final I localId;
    private final Set<I> servers;
    private final Duration electionTimeout;

    private LogSummary configurationIndex;

    public Configuration(I localId, Set<I> initialServers, Duration electionTimeout) {
        this.localId = localId;
        this.servers = new HashSet<>();
        this.servers.addAll(initialServers);
        this.electionTimeout = electionTimeout;
        this.configurationIndex = LogSummary.EMPTY;
    }

    public Set<I> getServers() {
        return Collections.unmodifiableSet(servers);
    }

    public Set<I> getOtherServerIds() {
        return servers.stream()
                .filter(id -> !Objects.equals(id, localId))
                .collect(Collectors.toSet());
    }

    public I getLocalId() {
        return localId;
    }

    public boolean isQuorum(Set<I> receivedVotes) {
        return receivedVotes.size() > (servers.size() / 2f);
    }

    public Duration getElectionTimeout() {
        return this.electionTimeout;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void entryAppended(int index, LogEntry logEntry) {
        if (logEntry instanceof ConfigurationEntry) {
            configurationIndex = new LogSummary(Optional.of(logEntry.getTerm()), index);
            servers.clear();
            servers.addAll((Set<I>) ((ConfigurationEntry) logEntry).getClusterMembers());
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void onSnapshotInstalled(Snapshot snapshot) {
        final LogSummary snapshotLastIndex = new LogSummary(Optional.of(snapshot.getLastTerm()), snapshot.getLastIndex());
        if (configurationIndex.compareTo(snapshotLastIndex) < 0
                && snapshot.getLastConfig() != null) {
            configurationIndex = snapshotLastIndex;
            servers.clear();
            final ConfigurationEntry lastConfig = snapshot.getLastConfig();
            servers.addAll((Set<I>) lastConfig.getClusterMembers());
        }
    }
}
