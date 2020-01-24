package au.id.tindall.distalg.raft.serverstates;

import au.id.tindall.distalg.raft.client.responses.PendingResponseRegistryFactory;
import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.replication.LogReplicatorFactory;
import au.id.tindall.distalg.raft.client.sessions.ClientSessionStore;
import au.id.tindall.distalg.raft.statemachine.CommandExecutor;

import java.io.Serializable;

public class ServerStateFactory<ID extends Serializable> {

    private final ID id;
    private final Log log;
    private final Cluster<ID> cluster;
    private final PendingResponseRegistryFactory pendingResponseRegistryFactory;
    private final LogReplicatorFactory<ID> logReplicatorFactory;
    private final ClientSessionStore clientSessionStore;
    private final CommandExecutor commandExecutor;

    public ServerStateFactory(ID id, Log log, Cluster<ID> cluster, PendingResponseRegistryFactory pendingResponseRegistryFactory,
                              LogReplicatorFactory<ID> logReplicatorFactory, ClientSessionStore clientSessionStore,
                              CommandExecutor commandExecutor) {
        this.id = id;
        this.log = log;
        this.cluster = cluster;
        this.pendingResponseRegistryFactory = pendingResponseRegistryFactory;
        this.logReplicatorFactory = logReplicatorFactory;
        this.clientSessionStore = clientSessionStore;
        this.commandExecutor = commandExecutor;
    }

    public Leader<ID> createLeader(Term currentTerm) {
        return new Leader<>(currentTerm, log, cluster, pendingResponseRegistryFactory.createPendingResponseRegistry(clientSessionStore, commandExecutor),
                logReplicatorFactory, this, clientSessionStore, id);
    }

    public Follower<ID> createFollower(Term currentTerm, ID currentLeader) {
        return createFollower(currentTerm, currentLeader, null);
    }

    public Follower<ID> createFollower(Term currentTerm, ID currentLeader, ID votedFor) {
        return new Follower<>(currentTerm, votedFor, log, cluster, this, currentLeader);
    }

    public Candidate<ID> createCandidate(Term currentTerm) {
        return new Candidate<>(currentTerm, log, cluster, id, this);
    }

    public ClientSessionStore getClientSessionStore() {
        return clientSessionStore;
    }
}
