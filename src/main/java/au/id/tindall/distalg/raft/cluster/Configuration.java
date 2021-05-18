package au.id.tindall.distalg.raft.cluster;

import au.id.tindall.distalg.raft.log.EntryAppendedEventHandler;
import au.id.tindall.distalg.raft.log.entries.ConfigurationEntry;
import au.id.tindall.distalg.raft.log.entries.LogEntry;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class Configuration<ID extends Serializable> implements EntryAppendedEventHandler {

    private final ID localId;
    private final Set<ID> servers;

    public Configuration(ID localId, Set<ID> initialServers) {
        this.localId = localId;
        this.servers = new HashSet<>();
        this.servers.addAll(initialServers);
    }

    public Set<ID> getServers() {
        return Collections.unmodifiableSet(servers);
    }

    public Set<ID> getOtherServerIds() {
        return servers.stream()
                .filter(id -> !Objects.equals(id, localId))
                .collect(Collectors.toSet());
    }

    public boolean isQuorum(Set<ID> receivedVotes) {
        return receivedVotes.size() > (servers.size() / 2f);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void entryAppended(int index, LogEntry logEntry) {
        if (logEntry instanceof ConfigurationEntry) {
            servers.clear();
            servers.addAll((Set<ID>) ((ConfigurationEntry) logEntry).getClusterMembers());
        }
    }
}
