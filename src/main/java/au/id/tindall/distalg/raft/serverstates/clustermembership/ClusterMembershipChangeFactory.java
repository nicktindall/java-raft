package au.id.tindall.distalg.raft.serverstates.clustermembership;

import au.id.tindall.distalg.raft.cluster.Configuration;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.replication.ReplicationManager;
import au.id.tindall.distalg.raft.state.PersistentState;

import java.io.Serializable;
import java.time.Instant;
import java.util.function.Supplier;

public class ClusterMembershipChangeFactory<ID extends Serializable> {

    private static final int DEFAULT_NUMBER_OF_CATCHUP_ROUNDS = 10;

    private final Log log;
    private final Configuration<ID> configuration;
    private final PersistentState<ID> persistentState;
    private final ReplicationManager<ID> replicationManager;
    private final Supplier<Instant> timeSource;

    public ClusterMembershipChangeFactory(Log log, Configuration<ID> configuration, PersistentState<ID> persistentState,
                                          ReplicationManager<ID> replicationManager, Supplier<Instant> timeSource) {
        this.log = log;
        this.configuration = configuration;
        this.persistentState = persistentState;
        this.replicationManager = replicationManager;
        this.timeSource = timeSource;
    }

    public AddServer<ID> createAddServer(ID newServerId) {
        return new AddServer<>(log, configuration, persistentState, replicationManager, newServerId, DEFAULT_NUMBER_OF_CATCHUP_ROUNDS, timeSource);
    }

    public RemoveServer<ID> createRemoveServer(ID serverToRemove) {
        return new RemoveServer<>(log, configuration, persistentState, replicationManager, serverToRemove, timeSource);
    }
}
