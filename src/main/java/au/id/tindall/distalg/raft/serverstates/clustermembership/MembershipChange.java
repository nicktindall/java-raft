package au.id.tindall.distalg.raft.serverstates.clustermembership;

import au.id.tindall.distalg.raft.cluster.Configuration;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.entries.ConfigurationEntry;
import au.id.tindall.distalg.raft.replication.ReplicationManager;
import au.id.tindall.distalg.raft.rpc.client.ClientResponseMessage;
import au.id.tindall.distalg.raft.state.PersistentState;

import java.io.Closeable;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public abstract class MembershipChange<I, R extends ClientResponseMessage<I>> implements Closeable {
    protected static final int NOT_SET = Integer.MIN_VALUE;
    private final PersistentState<I> persistentState;
    protected final Log log;
    protected final Configuration<I> configuration;
    protected final I serverId;
    protected final CompletableFuture<R> responseFuture;
    protected final Supplier<Instant> timeSource;
    protected final ReplicationManager<I> replicationManager;
    protected Instant lastProgressTime;
    protected int finishedAtIndex = NOT_SET;
    protected boolean finished;

    MembershipChange(Log log, Configuration<I> configuration, PersistentState<I> persistentState, ReplicationManager<I> replicationManager, I serverId, Supplier<Instant> timeSource) {
        this.log = log;
        this.configuration = configuration;
        this.persistentState = persistentState;
        this.serverId = serverId;
        this.timeSource = timeSource;
        this.replicationManager = replicationManager;
        this.responseFuture = new CompletableFuture<>();
    }

    void start() {
        this.lastProgressTime = timeSource.get();
        onStart();
    }

    protected abstract void onStart();

    void matchIndexAdvanced(I serverId, int lastAppendedIndex) {
        if (finishedAtIndex != NOT_SET) {
            return;
        }
        if (this.serverId.equals(serverId)) {
            lastProgressTime = timeSource.get();
            final R result = matchIndexAdvancedInternal(lastAppendedIndex);
            if (result != null) {
                responseFuture.complete(result);
                finished = true;
            }
        }
    }

    protected abstract R matchIndexAdvancedInternal(int lastAppendedIndex);

    void logMessageFromFollower(I followerId) {
        if (this.serverId.equals(followerId)) {
            lastProgressTime = timeSource.get();
        }
    }

    void entryCommitted(int index) {
        final R result = entryCommittedInternal(index);
        if (result != null) {
            responseFuture.complete(result);
            finished = true;
        }
    }

    protected abstract R entryCommittedInternal(int index);

    protected abstract R timeoutIfSlow();

    CompletableFuture<R> getResponseFuture() {
        return responseFuture;
    }

    public boolean isFinished() {
        if (!finished) {
            R result = timeoutIfSlow();
            if (result != null) {
                responseFuture.complete(result);
                finished = true;
            }
        }
        return finished;
    }

    protected int addServerToConfig(I serverId) {
        Set<Object> newServers = new HashSet<>(configuration.getServers());
        newServers.add(serverId);
        log.appendEntries(log.getLastLogIndex(), List.of(
                new ConfigurationEntry(persistentState.getCurrentTerm(), newServers)
        ));
        return log.getLastLogIndex();
    }

    protected int removeServerFromConfig(I serverId) {
        Set<Object> newServers = new HashSet<>(configuration.getServers());
        newServers.remove(serverId);
        log.appendEntries(log.getLastLogIndex(), List.of(
                new ConfigurationEntry(persistentState.getCurrentTerm(), newServers)
        ));
        return log.getLastLogIndex();
    }
}
