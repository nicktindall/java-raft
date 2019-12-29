package au.id.tindall.distalg.raft.serverstates;

import java.io.Serializable;

import au.id.tindall.distalg.raft.client.PendingResponseRegistryFactory;
import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.replication.LogReplicatorFactory;
import au.id.tindall.distalg.raft.statemachine.ClientSessionStore;

public class ServerStateFactory<ID extends Serializable> {

    private final ID id;
    private final Log log;
    private final Cluster<ID> cluster;
    private final PendingResponseRegistryFactory pendingResponseRegistryFactory;
    private final LogReplicatorFactory<ID> logReplicatorFactory;
    private final ClientSessionStore clientSessionStore;

    public ServerStateFactory(ID id, Log log, Cluster<ID> cluster, PendingResponseRegistryFactory pendingResponseRegistryFactory, LogReplicatorFactory<ID> logReplicatorFactory, ClientSessionStore clientSessionStore) {
        this.id = id;
        this.log = log;
        this.cluster = cluster;
        this.pendingResponseRegistryFactory = pendingResponseRegistryFactory;
        this.logReplicatorFactory = logReplicatorFactory;
        this.clientSessionStore = clientSessionStore;
    }

    public Leader<ID> createLeader(Term currentTerm) {
        return new Leader<>(currentTerm, log, cluster, pendingResponseRegistryFactory, logReplicatorFactory, this);
    }

    public Follower<ID> createFollower(Term currentTerm) {
        return createFollower(currentTerm, null);
    }

    public Follower<ID> createFollower(Term currentTerm, ID votedFor) {
        return new Follower<>(currentTerm, votedFor, log, cluster, this);
    }

    public Candidate<ID> createCandidate(Term currentTerm) {
        return new Candidate<>(currentTerm, log, cluster, id, this);
    }

    public ClientSessionStore getClientSessionStore() {
        return clientSessionStore;
    }
}
