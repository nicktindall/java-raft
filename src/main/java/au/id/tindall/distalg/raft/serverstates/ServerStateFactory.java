package au.id.tindall.distalg.raft.serverstates;

import au.id.tindall.distalg.raft.client.responses.PendingResponseRegistryFactory;
import au.id.tindall.distalg.raft.client.sessions.ClientSessionStore;
import au.id.tindall.distalg.raft.cluster.Configuration;
import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.elections.ElectionScheduler;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.processors.ProcessorManager;
import au.id.tindall.distalg.raft.processors.RaftProcessorGroup;
import au.id.tindall.distalg.raft.replication.ReplicationManager;
import au.id.tindall.distalg.raft.replication.ReplicationManagerFactory;
import au.id.tindall.distalg.raft.serverstates.clustermembership.ClusterMembershipChangeManager;
import au.id.tindall.distalg.raft.serverstates.clustermembership.ClusterMembershipChangeManagerFactory;
import au.id.tindall.distalg.raft.serverstates.leadershiptransfer.LeadershipTransferFactory;
import au.id.tindall.distalg.raft.state.PersistentState;
import au.id.tindall.distalg.raft.statemachine.CommandExecutor;
import au.id.tindall.distalg.raft.timing.TimingWrappers;

import java.io.Closeable;
import java.io.Serializable;

import static au.id.tindall.distalg.raft.util.Closeables.closeQuietly;

public class ServerStateFactory<I extends Serializable> implements Closeable {

    private static final int WARNING_THRESHOLD_MILLIS = 10;
    private final Log log;
    private final Cluster<I> cluster;
    private final PendingResponseRegistryFactory pendingResponseRegistryFactory;
    private final ClientSessionStore clientSessionStore;
    private final CommandExecutor commandExecutor;
    private final ElectionScheduler electionScheduler;
    private final PersistentState<I> persistentState;
    private final LeadershipTransferFactory<I> leadershipTransferFactory;
    private final ReplicationManagerFactory<I> replicationManagerFactory;
    private final ClusterMembershipChangeManagerFactory<I> clusterMembershipChangeManagerFactory;
    private final ProcessorManager<RaftProcessorGroup> processorManager;
    private final boolean timing;
    private final Configuration<I> configuration;

    public ServerStateFactory(PersistentState<I> persistentState, Log log, Cluster<I> cluster, PendingResponseRegistryFactory pendingResponseRegistryFactory,
                              ClientSessionStore clientSessionStore, CommandExecutor commandExecutor, ElectionScheduler electionScheduler,
                              LeadershipTransferFactory<I> leadershipTransferFactory, ReplicationManagerFactory<I> replicationManagerFactory,
                              ClusterMembershipChangeManagerFactory<I> clusterMembershipChangeManagerFactory, boolean timing, ProcessorManager<RaftProcessorGroup> processorManager, Configuration<I> configuration) {
        this.persistentState = persistentState;
        this.log = log;
        this.cluster = cluster;
        this.pendingResponseRegistryFactory = pendingResponseRegistryFactory;
        this.clientSessionStore = clientSessionStore;
        this.commandExecutor = commandExecutor;
        this.electionScheduler = electionScheduler;
        this.leadershipTransferFactory = leadershipTransferFactory;
        this.replicationManagerFactory = replicationManagerFactory;
        this.clusterMembershipChangeManagerFactory = clusterMembershipChangeManagerFactory;
        this.timing = timing;
        this.processorManager = processorManager;
        this.configuration = configuration;
    }

    public ServerState<I> createLeader() {
        final ReplicationManager<I> replicationManager = replicationManagerFactory.createReplicationManager();
        final ClusterMembershipChangeManager<I> clusterMembershipChangeManager = clusterMembershipChangeManagerFactory.createChangeManager(replicationManager);
        replicationManager.addMatchIndexAdvancedListener(clusterMembershipChangeManager);
        final Leader<I> leaderState = new Leader<>(persistentState, log, cluster, pendingResponseRegistryFactory.createPendingResponseRegistry(clientSessionStore, commandExecutor),
                this, replicationManager, clientSessionStore, leadershipTransferFactory.create(replicationManager), clusterMembershipChangeManager, processorManager);
        return timing ? TimingWrappers.wrap(leaderState, WARNING_THRESHOLD_MILLIS) : leaderState;
    }

    public ServerState<I> createInitialState() {
        return createFollower(null);
    }

    public ServerState<I> createFollower(I currentLeader) {
        final Follower<I> followerState = new Follower<>(persistentState, log, cluster, this, currentLeader, electionScheduler);
        return timing ? TimingWrappers.wrap(followerState, WARNING_THRESHOLD_MILLIS) : followerState;
    }

    public ServerState<I> createCandidate() {
        final Candidate<I> candidateState = new Candidate<>(persistentState, log, cluster, this, electionScheduler, configuration);
        return timing ? TimingWrappers.wrap(candidateState, WARNING_THRESHOLD_MILLIS) : candidateState;
    }

    public ClientSessionStore getClientSessionStore() {
        return clientSessionStore;
    }

    @Override
    public void close() {
        closeQuietly(electionScheduler);
    }
}
