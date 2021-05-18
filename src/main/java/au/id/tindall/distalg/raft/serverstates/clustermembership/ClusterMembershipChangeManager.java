package au.id.tindall.distalg.raft.serverstates.clustermembership;

import au.id.tindall.distalg.raft.cluster.Configuration;
import au.id.tindall.distalg.raft.log.EntryCommittedEventHandler;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.entries.LogEntry;
import au.id.tindall.distalg.raft.replication.ReplicationManager;
import au.id.tindall.distalg.raft.rpc.clustermembership.AddServerResponse;
import au.id.tindall.distalg.raft.rpc.clustermembership.RemoveServerResponse;
import au.id.tindall.distalg.raft.state.PersistentState;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public class ClusterMembershipChangeManager<ID extends Serializable> implements EntryCommittedEventHandler {

    private final Log log;
    private final PersistentState<ID> persistentState;
    private final ReplicationManager<ID> replicationManager;
    private final Queue<MembershipChange<ID, ?>> membershipChangeQueue;
    private final Duration maxDelayBetweenMessages;
    private final Configuration<ID> configuration;
    private final Supplier<Instant> timeSource;
    private MembershipChange<ID, ?> currentMembershipChange;

    public ClusterMembershipChangeManager(Log log, PersistentState<ID> persistentState,
                                          ReplicationManager<ID> replicationManager,
                                          Configuration<ID> configuration,
                                          long maxDelayBetweenMessagesMilliseconds,
                                          Supplier<Instant> timeSource) {
        this.log = log;
        this.persistentState = persistentState;
        this.replicationManager = replicationManager;
        this.configuration = configuration;
        this.maxDelayBetweenMessages = Duration.ofMillis(maxDelayBetweenMessagesMilliseconds);
        this.timeSource = timeSource;
        membershipChangeQueue = new LinkedList<>();
        currentMembershipChange = null;
    }

    public CompletableFuture<AddServerResponse> addServer(ID newServerId) {
        final AddServer<ID> addServer = new AddServer<>(log, configuration, persistentState,
                replicationManager, newServerId, maxDelayBetweenMessages, timeSource);
        membershipChangeQueue.add(addServer);
        startNextMembershipChangeIfReady();
        return addServer.getResponseFuture();
    }

    public CompletableFuture<RemoveServerResponse> removeServer(ID serverId) {
        final RemoveServer<ID> removeServer = new RemoveServer<>(log, configuration, persistentState,
                serverId);
        membershipChangeQueue.add(removeServer);
        startNextMembershipChangeIfReady();
        return removeServer.getResponseFuture();
    }

    private void startNextMembershipChangeIfReady() {
        if (currentMembershipChange == null || currentMembershipChange.isFinished()) {
            final MembershipChange<ID, ?> nextChange = membershipChangeQueue.remove();
            if (nextChange != null) {
                currentMembershipChange = nextChange;
                currentMembershipChange.start();
            }
        }
    }

    @Override
    public void entryCommitted(int index, LogEntry logEntry) {
        if (currentMembershipChange != null) {
            currentMembershipChange.entryCommitted(index);
            startNextMembershipChangeIfReady();
        }
    }
}
