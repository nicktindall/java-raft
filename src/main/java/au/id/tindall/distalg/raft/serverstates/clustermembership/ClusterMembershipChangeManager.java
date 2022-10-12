package au.id.tindall.distalg.raft.serverstates.clustermembership;

import au.id.tindall.distalg.raft.log.EntryCommittedEventHandler;
import au.id.tindall.distalg.raft.log.entries.LogEntry;
import au.id.tindall.distalg.raft.replication.MatchIndexAdvancedListener;
import au.id.tindall.distalg.raft.rpc.clustermembership.AddServerResponse;
import au.id.tindall.distalg.raft.rpc.clustermembership.RemoveServerResponse;

import java.io.Closeable;
import java.io.Serializable;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

import static au.id.tindall.distalg.raft.util.Closeables.closeQuietly;

public class ClusterMembershipChangeManager<ID extends Serializable> implements EntryCommittedEventHandler, Closeable, MatchIndexAdvancedListener<ID> {

    private final Queue<MembershipChange<ID, ?>> membershipChangeQueue;
    private final ClusterMembershipChangeFactory<ID> clusterMembershipChangeFactory;
    private MembershipChange<ID, ?> currentMembershipChange;

    public ClusterMembershipChangeManager(ClusterMembershipChangeFactory<ID> clusterMembershipChangeFactory) {
        this.clusterMembershipChangeFactory = clusterMembershipChangeFactory;
        membershipChangeQueue = new LinkedList<>();
        currentMembershipChange = null;
    }

    public CompletableFuture<AddServerResponse> addServer(ID newServerId) {
        final AddServer<ID> addServer = clusterMembershipChangeFactory.createAddServer(newServerId);
        membershipChangeQueue.add(addServer);
        startNextMembershipChangeIfReady();
        return addServer.getResponseFuture();
    }

    public CompletableFuture<RemoveServerResponse> removeServer(ID serverId) {
        final RemoveServer<ID> removeServer = clusterMembershipChangeFactory.createRemoveServer(serverId);
        membershipChangeQueue.add(removeServer);
        startNextMembershipChangeIfReady();
        return removeServer.getResponseFuture();
    }

    @Override
    public void matchIndexAdvanced(ID followerId, int newMatchIndex) {
        if (currentMembershipChange != null && !currentMembershipChange.isFinished()) {
            currentMembershipChange.matchIndexAdvanced(followerId, newMatchIndex);
        }
    }

    public void logMessageFromFollower(ID followerId) {
        if (currentMembershipChange != null && !currentMembershipChange.isFinished()) {
            currentMembershipChange.logMessageFromFollower(followerId);
        }
    }

    private void startNextMembershipChangeIfReady() {
        if (currentMembershipChange == null || currentMembershipChange.isFinished()) {
            final MembershipChange<ID, ?> nextChange = membershipChangeQueue.poll();
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

    @Override
    public void close() {
        if (currentMembershipChange != null) {
            closeQuietly(currentMembershipChange);
            currentMembershipChange = null;
        }
        closeQuietly(membershipChangeQueue);
    }
}
