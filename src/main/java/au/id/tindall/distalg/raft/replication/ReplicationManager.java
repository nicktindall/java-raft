package au.id.tindall.distalg.raft.replication;

import au.id.tindall.distalg.raft.cluster.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class ReplicationManager<I extends Serializable> implements MatchIndexAdvancedListener<I> {
    private static final Logger LOGGER = LogManager.getLogger();

    private final Configuration<I> configuration;
    private final SingleClientReplicatorFactory<I> replicatorFactory;
    private Map<I, SingleClientReplicator<I>> replicators;
    private final List<MatchIndexAdvancedListener<I>> matchIndexAdvancedListeners;
    private boolean started;

    public ReplicationManager(Configuration<I> configuration, SingleClientReplicatorFactory<I> replicatorFactory) {
        this.configuration = configuration;
        this.replicatorFactory = replicatorFactory;
        this.started = false;
        this.matchIndexAdvancedListeners = new ArrayList<>();
    }

    public void start() {
        assert replicators == null;
        assert !started;
        replicators = new ConcurrentHashMap<>(configuration.getOtherServerIds().stream()
                .collect(toMap(identity(), followerId -> replicatorFactory.createReplicator(configuration.getLocalId(), followerId, this))));
        replicators.values().forEach(SingleClientReplicator::start);
        started = true;
    }

    public void stop() {
        assert started;
        replicators.values().forEach(SingleClientReplicator::stop);
        started = false;
    }

    public int getMatchIndex(I serverId) {
        return replicators.get(serverId).getMatchIndex();
    }

    public void logSuccessResponse(I serverId, int lastAppendedIndex) {
        final SingleClientReplicator<I> replicator = replicators.get(serverId);
        if (replicator != null) {
            replicator.logSuccessResponse(lastAppendedIndex);
        } else {
            LOGGER.debug("Tried to log success response for missing peer: {}", serverId);
        }
    }

    public void logSuccessSnapshotResponse(I serverId, int lastIndex, int lastOffset) {
        final SingleClientReplicator<I> replicator = replicators.get(serverId);
        if (replicator != null) {
            replicator.logSuccessSnapshotResponse(lastIndex, lastOffset);
        } else {
            LOGGER.debug("Tried to log success response for missing peer: {}", serverId);
        }
    }

    public int getNextIndex(I serverId) {
        return replicators.get(serverId).getNextIndex();
    }

    public void replicateIfTrailingIndex(I serverId, int lastLogIndex) {
        final SingleClientReplicator<I> replicator = replicators.get(serverId);
        if (replicator != null && replicator.getNextIndex() <= lastLogIndex) {
            replicator.replicate();
        }
    }

    public void replicate(I serverId) {
        final SingleClientReplicator<I> idSingleClientReplicator = replicators.get(serverId);
        if (idSingleClientReplicator != null) {
            idSingleClientReplicator.replicate();
        }
    }

    public void replicate() {
        replicators.values().forEach(SingleClientReplicator::replicate);
    }

    public void logFailedResponse(I serverId, Integer earliestPossibleMatchIndex) {
        final SingleClientReplicator<I> replicator = replicators.get(serverId);
        if (replicator != null) {
            replicator.logFailedResponse(earliestPossibleMatchIndex);
        } else {
            LOGGER.debug("Tried to log failed response for missing peer: {}", serverId);
        }
    }

    public List<Integer> getFollowerMatchIndices() {
        return configuration.getOtherServerIds().stream()
                .map(followerId -> replicators.get(followerId))
                .map(SingleClientReplicator::getMatchIndex)
                .collect(toList());
    }

    public void startReplicatingTo(I followerId) {
        LOGGER.debug("Starting replicating to {}", followerId);
        final SingleClientReplicator<I> logReplicator = replicatorFactory.createReplicator(configuration.getLocalId(), followerId, this);
        replicators.put(followerId, logReplicator);
        logReplicator.start();
        logReplicator.replicate();
    }

    public void stopReplicatingTo(I serverId) {
        final SingleClientReplicator<I> replicator = replicators.remove(serverId);
        if (replicator != null) {
            replicator.stop();
        }
    }

    @Override
    public void matchIndexAdvanced(I followerId, int newMatchIndex) {
        matchIndexAdvancedListeners.forEach(l -> l.matchIndexAdvanced(followerId, newMatchIndex));
    }

    public void addMatchIndexAdvancedListener(MatchIndexAdvancedListener<I> matchIndexAdvancedListener) {
        matchIndexAdvancedListeners.add(matchIndexAdvancedListener);
    }
}
