package au.id.tindall.distalg.raft.serverstates;

import au.id.tindall.distalg.raft.client.responses.PendingResponseRegistryFactory;
import au.id.tindall.distalg.raft.client.sessions.ClientSessionStore;
import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.driver.ElectionScheduler;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.replication.LogReplicatorFactory;
import au.id.tindall.distalg.raft.state.PersistentState;
import au.id.tindall.distalg.raft.statemachine.CommandExecutor;

import java.io.Serializable;

public class ServerStateFactory<ID extends Serializable> {

    private final Log log;
    private final Cluster<ID> cluster;
    private final PendingResponseRegistryFactory pendingResponseRegistryFactory;
    private final LogReplicatorFactory<ID> logReplicatorFactory;
    private final ClientSessionStore clientSessionStore;
    private final CommandExecutor commandExecutor;
    private final ElectionScheduler<ID> electionScheduler;
    private final PersistentState<ID> persistentState;

    public ServerStateFactory(PersistentState<ID> persistentState, Log log, Cluster<ID> cluster, PendingResponseRegistryFactory pendingResponseRegistryFactory,
                              LogReplicatorFactory<ID> logReplicatorFactory, ClientSessionStore clientSessionStore,
                              CommandExecutor commandExecutor, ElectionScheduler<ID> electionScheduler) {
        this.persistentState = persistentState;
        this.log = log;
        this.cluster = cluster;
        this.pendingResponseRegistryFactory = pendingResponseRegistryFactory;
        this.logReplicatorFactory = logReplicatorFactory;
        this.clientSessionStore = clientSessionStore;
        this.commandExecutor = commandExecutor;
        this.electionScheduler = electionScheduler;
    }

    public Leader<ID> createLeader() {
        return new Leader<>(persistentState, log, cluster, pendingResponseRegistryFactory.createPendingResponseRegistry(clientSessionStore, commandExecutor),
                logReplicatorFactory, this, clientSessionStore);
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
