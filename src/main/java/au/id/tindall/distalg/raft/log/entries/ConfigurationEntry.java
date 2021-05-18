package au.id.tindall.distalg.raft.log.entries;

import au.id.tindall.distalg.raft.log.Term;

import java.io.Serializable;
import java.util.Set;

public class ConfigurationEntry extends LogEntry {

    private final Set<Serializable> clusterMembers;

    public ConfigurationEntry(Term term, Set<Serializable> clusterMembers) {
        super(term);
        this.clusterMembers = clusterMembers;
    }

    public Set<Serializable> getClusterMembers() {
        return clusterMembers;
    }
}
