package au.id.tindall.distalg.raft.serverstates;

import au.id.tindall.distalg.raft.client.responses.PendingResponseRegistryFactory;
import au.id.tindall.distalg.raft.client.sessions.ClientSessionStore;
import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.elections.ElectionScheduler;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.replication.ReplicationManager;
import au.id.tindall.distalg.raft.replication.ReplicationManagerFactory;
import au.id.tindall.distalg.raft.serverstates.leadershiptransfer.LeadershipTransferFactory;
import au.id.tindall.distalg.raft.state.PersistentState;
import au.id.tindall.distalg.raft.statemachine.CommandExecutor;

import java.io.Serializable;

public class ServerStateFactory<ID extends Serializable> {

    private final Log log;
    private final Cluster<ID> cluster;
    private final PendingResponseRegistryFactory pendingResponseRegistryFactory;
    private final ClientSessionStore clientSessionStore;
    private final CommandExecutor commandExecutor;
    private final ElectionScheduler<ID> electionScheduler;
    private final PersistentState<ID> persistentState;
    private final LeadershipTransferFactory<ID> leadershipTransferFactory;
    private final ReplicationManagerFactory<ID> replicationManagerFactory;

    public ServerStateFactory(PersistentState<ID> persistentState, Log log, Cluster<ID> cluster, PendingResponseRegistryFactory pendingResponseRegistryFactory,
                              ClientSessionStore clientSessionStore, CommandExecutor commandExecutor, ElectionScheduler<ID> electionScheduler,
                              LeadershipTransferFactory<ID> leadershipTransferFactory, ReplicationManagerFactory<ID> replicationManagerFactory) {
        this.persistentState = persistentState;
        this.log = log;
        this.cluster = cluster;
        this.pendingResponseRegistryFactory = pendingResponseRegistryFactory;
        this.clientSessionStore = clientSessionStore;
        this.commandExecutor = commandExecutor;
        this.electionScheduler = electionScheduler;
        this.leadershipTransferFactory = leadershipTransferFactory;
        this.replicationManagerFactory = replicationManagerFactory;
    }

    public Leader<ID> createLeader() {
        final ReplicationManager<ID> replicationManager = replicationManagerFactory.createReplicationManager();
        return new Leader<>(persistentState, log, cluster, pendingResponseRegistryFactory.createPendingResponseRegistry(clientSessionStore, commandExecutor),
                this, replicationManager, clientSessionStore, leadershipTransferFactory.create(replicationManager));
    }

    public Follower<ID> createInitialState() {
        return createFollower(null);
    }

    public Follower<ID> createFollower(ID currentLeader) {
        return new Follower<>(persistentState, log, cluster, this, currentLeader, electionScheduler);
    }

    public Candidate<ID> createCandidate() {
        return new Candidate<>(persistentState, log, cluster, this, electionScheduler);
    }

    public ClientSessionStore getClientSessionStore() {
        return clientSessionStore;
    }
}
