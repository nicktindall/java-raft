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
    private final LogReplicatorFactory<ID> logReplicatorFactory;
    private Map<ID, LogReplicator<ID>> replicators;
    private boolean started;

    public ReplicationManager(Configuration<ID> configuration, LogReplicatorFactory<ID> logReplicatorFactory) {
        this.configuration = configuration;
        this.logReplicatorFactory = logReplicatorFactory;
        this.started = false;
    }

    public void start() {
        assert replicators == null;
        assert !started;
        replicators = new ConcurrentHashMap<>(configuration.getOtherServerIds().stream()
                .collect(toMap(identity(), logReplicatorFactory::createLogReplicator)));
        replicators.values().forEach(LogReplicator::start);
        started = true;
    }

    public void stop() {
        assert started;
        replicators.values().forEach(LogReplicator::stop);
        started = false;
    }

    public int getMatchIndex(ID serverId) {
        return replicators.get(serverId).getMatchIndex();
    }

    public void logSuccessResponse(ID serverId, int lastAppendedIndex) {
        final LogReplicator<ID> replicator = replicators.get(serverId);
        if (replicator != null) {
            replicator.logSuccessResponse(lastAppendedIndex);
        } else {
            LOGGER.warn("Tried to log success response for missing peer: {}", serverId);
        }
    }

    public int getNextIndex(ID serverId) {
        return replicators.get(serverId).getNextIndex();
    }

    public void replicateIfTrailingIndex(ID serverId, int lastLogIndex) {
        final LogReplicator<ID> replicator = replicators.get(serverId);
        if (replicator != null && replicator.getNextIndex() <= lastLogIndex) {
            replicator.replicate();
        }
    }

    public void replicate(ID serverId) {
        replicators.get(serverId).replicate();
    }

    public void replicate() {
        replicators.values().forEach(LogReplicator::replicate);
    }

    public void logFailedResponse(ID serverId, Integer followerLastLogIndex) {
        final LogReplicator<ID> replicator = replicators.get(serverId);
        if (replicator != null) {
            replicator.logFailedResponse(followerLastLogIndex);
        } else {
            LOGGER.warn("Tried to log failed response for missing peer: {}", serverId);
        }
    }

    public List<Integer> getFollowerMatchIndices() {
        return configuration.getOtherServerIds().stream()
                .map(followerId -> replicators.get(followerId))
                .map(LogReplicator::getMatchIndex)
                .collect(toList());
    }

    public void startReplicatingTo(ID serverId) {
        final LogReplicator<ID> logReplicator = logReplicatorFactory.createLogReplicator(serverId);
        replicators.put(serverId, logReplicator);
        logReplicator.start();
        logReplicator.replicate();
    }

    public void stopReplicatingTo(ID serverId) {
        final LogReplicator<ID> replicator = replicators.remove(serverId);
        if (replicator != null) {
            replicator.stop();
        }
    }
}
