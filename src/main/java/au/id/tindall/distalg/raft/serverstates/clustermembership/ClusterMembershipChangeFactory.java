package au.id.tindall.distalg.raft.serverstates.clustermembership;

import au.id.tindall.distalg.raft.cluster.Configuration;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.replication.ReplicationManager;
import au.id.tindall.distalg.raft.state.PersistentState;

import java.io.Serializable;
import java.time.Instant;
import java.util.function.Supplier;

public class ClusterMembershipChangeFactory<I extends Serializable> {

    private static final int DEFAULT_NUMBER_OF_CATCHUP_ROUNDS = 10;

    private final Log log;
    private final Configuration<I> configuration;
    private final PersistentState<I> persistentState;
    private final ReplicationManager<I> replicationManager;
    private final Supplier<Instant> timeSource;

    public ClusterMembershipChangeFactory(Log log, Configuration<I> configuration, PersistentState<I> persistentState,
                                          ReplicationManager<I> replicationManager, Supplier<Instant> timeSource) {
        this.log = log;
        this.configuration = configuration;
        this.persistentState = persistentState;
        this.replicationManager = replicationManager;
        this.timeSource = timeSource;
    }

    public AddServer<I> createAddServer(I newServerId) {
        return new AddServer<>(log, configuration, persistentState, replicationManager, newServerId, DEFAULT_NUMBER_OF_CATCHUP_ROUNDS, timeSource);
    }

    public RemoveServer<I> createRemoveServer(I serverToRemove) {
        return new RemoveServer<>(log, configuration, persistentState, replicationManager, serverToRemove, timeSource);
    }
}
