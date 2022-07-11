package au.id.tindall.distalg.raft.replication;

import au.id.tindall.distalg.raft.cluster.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class ReplicationManager<ID extends Serializable> {
    private static final Logger LOGGER = LogManager.getLogger();

    private final Configuration<ID> configuration;
    private final SingleClientReplicatorFactory<ID> replicatorFactory;
    private Map<ID, SingleClientReplicator<ID>> replicators;
    private boolean started;

    public ReplicationManager(Configuration<ID> configuration, SingleClientReplicatorFactory<ID> replicatorFactory) {
        this.configuration = configuration;
        this.replicatorFactory = replicatorFactory;
        this.started = false;
    }

    public void start() {
        assert replicators == null;
        assert !started;
        replicators = new ConcurrentHashMap<>(configuration.getOtherServerIds().stream()
                .collect(toMap(identity(), followerId -> replicatorFactory.createReplicator(configuration.getLocalId(), followerId))));
        replicators.values().forEach(SingleClientReplicator::start);
        started = true;
    }

    public void stop() {
        assert started;
        replicators.values().forEach(SingleClientReplicator::stop);
        started = false;
    }

    public int getMatchIndex(ID serverId) {
        return replicators.get(serverId).getMatchIndex();
    }

    public void logSuccessResponse(ID serverId, int lastAppendedIndex) {
        final SingleClientReplicator<ID> replicator = replicators.get(serverId);
        if (replicator != null) {
            replicator.logSuccessResponse(lastAppendedIndex);
        } else {
            LOGGER.warn("Tried to log success response for missing peer: {}", serverId);
        }
    }

    public void logSuccessSnapshotResponse(ID serverId, int lastIndex, int lastOffset) {
        final SingleClientReplicator<ID> replicator = replicators.get(serverId);
        if (replicator != null) {
            replicator.logSuccessSnapshotResponse(lastIndex, lastOffset);
        } else {
            LOGGER.warn("Tried to log success response for missing peer: {}", serverId);
        }
    }

    public int getNextIndex(ID serverId) {
        return replicators.get(serverId).getNextIndex();
    }

    public void replicateIfTrailingIndex(ID serverId, int lastLogIndex) {
        final SingleClientReplicator<ID> replicator = replicators.get(serverId);
        if (replicator != null && replicator.getNextIndex() <= lastLogIndex) {
            replicator.replicate();
        }
    }

    public void replicate(ID serverId) {
        final SingleClientReplicator<ID> idSingleClientReplicator = replicators.get(serverId);
        if (idSingleClientReplicator != null) {
            idSingleClientReplicator.replicate();
        }
    }

    public void replicate() {
        replicators.values().forEach(SingleClientReplicator::replicate);
    }

    public void logFailedResponse(ID serverId, Integer earliestPossibleMatchIndex) {
        final SingleClientReplicator<ID> replicator = replicators.get(serverId);
        if (replicator != null) {
            replicator.logFailedResponse(earliestPossibleMatchIndex);
        } else {
            LOGGER.warn("Tried to log failed response for missing peer: {}", serverId);
        }
    }

    public List<Integer> getFollowerMatchIndices() {
        return configuration.getOtherServerIds().stream()
                .map(followerId -> replicators.get(followerId))
                .map(SingleClientReplicator::getMatchIndex)
                .collect(toList());
    }

    public void startReplicatingTo(ID followerId) {
        LOGGER.warn("Starting replicating to " + followerId);
        final SingleClientReplicator<ID> logReplicator = replicatorFactory.createReplicator(configuration.getLocalId(), followerId);
        replicators.put(followerId, logReplicator);
        logReplicator.start();
        logReplicator.replicate();
    }

    public void stopReplicatingTo(ID serverId) {
        final SingleClientReplicator<ID> replicator = replicators.remove(serverId);
        if (replicator != null) {
            replicator.stop();
        }
    }
}
