package au.id.tindall.distalg.raft.serverstates.clustermembership;

import au.id.tindall.distalg.raft.cluster.Configuration;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.entries.ConfigurationEntry;
import au.id.tindall.distalg.raft.rpc.clustermembership.ClusterMembershipResponse;
import au.id.tindall.distalg.raft.state.PersistentState;

import java.io.Closeable;
import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

abstract public class MembershipChange<ID extends Serializable, R extends ClusterMembershipResponse> implements Closeable {
    private final Log log;
    protected final Configuration<ID> configuration;
    private final PersistentState<ID> persistentState;
    protected final ID serverId;
    protected final CompletableFuture<R> responseFuture;
    protected int finishedAtIndex = -1;
    protected boolean finished;

    MembershipChange(Log log, Configuration<ID> configuration, PersistentState<ID> persistentState, ID serverId) {
        this.log = log;
        this.configuration = configuration;
        this.persistentState = persistentState;
        this.serverId = serverId;
        this.responseFuture = new CompletableFuture<>();
    }

    abstract void start();

    void logSuccessResponse(ID serverId, int lastAppendedIndex) {
        if (finishedAtIndex >= 0) {
            return;
        }
        final R result = logSuccessResponseInternal(serverId, lastAppendedIndex);
        if (result != null) {
            responseFuture.complete(result);
            finished = true;
        }
    }

    R logSuccessResponseInternal(ID serverId, int lastAppendedIndex) {
        // Do nothing by default
        return null;
    }

    void logFailureResponse(ID serverId) {
        if (finishedAtIndex >= 0) {
            return;
        }
        final R result = logFailureResponseInternal(serverId);
        if (result != null) {
            responseFuture.complete(result);
            finished = true;
        }
    }

    R logFailureResponseInternal(ID serverId) {
        // Do nothing by default
        return null;
    }


    void entryCommitted(int index) {
        final R result = entryCommittedInternal(index);
        if (result != null) {
            responseFuture.complete(result);
            finished = true;
        }
    }

    protected abstract R entryCommittedInternal(int index);

    protected R timeoutIfSlow() {
        // Only adds timeout
        return null;
    }

    CompletableFuture<R> getResponseFuture() {
        return responseFuture;
    }

    public boolean isFinished() {
        R result = timeoutIfSlow();
        if (result != null) {
            responseFuture.complete(result);
            finished = true;
        }
        return finished;
    }

    protected int addServerToConfig(ID serverId) {
        Set<Serializable> newServers = new HashSet<>(configuration.getServers());
        newServers.add(serverId);
        log.appendEntries(log.getLastLogIndex(), List.of(
                new ConfigurationEntry(persistentState.getCurrentTerm(), newServers)
        ));
        return log.getLastLogIndex();
    }

    protected int removeServerFromConfig(ID serverId) {
        Set<Serializable> newServers = new HashSet<>(configuration.getServers());
        newServers.remove(serverId);
        log.appendEntries(log.getLastLogIndex(), List.of(
                new ConfigurationEntry(persistentState.getCurrentTerm(), newServers)
        ));
        return log.getLastLogIndex();
    }

    public abstract void logSnapshotResponse(ID serverId);
}
