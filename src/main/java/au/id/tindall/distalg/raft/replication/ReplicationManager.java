package au.id.tindall.distalg.raft.replication;

import au.id.tindall.distalg.raft.comms.Cluster;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class ReplicationManager<ID extends Serializable> {

    private final Cluster<ID> cluster;
    private final LogReplicatorFactory<ID> logReplicatorFactory;
    private Map<ID, LogReplicator<ID>> replicators;
    private boolean started;

    public ReplicationManager(Cluster<ID> cluster, LogReplicatorFactory<ID> logReplicatorFactory) {
        this.cluster = cluster;
        this.logReplicatorFactory = logReplicatorFactory;
        this.started = false;
    }

    public void start() {
        assert replicators == null;
        assert !started;
        replicators = new HashMap<>(cluster.getOtherMemberIds().stream()
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
        replicators.get(serverId).logSuccessResponse(lastAppendedIndex);
    }

    public int getNextIndex(ID serverId) {
        return replicators.get(serverId).getNextIndex();
    }

    public void replicate(ID serverId) {
        replicators.get(serverId).replicate();
    }

    public void replicate() {
        replicators.values().forEach(LogReplicator::replicate);
    }

    public void logFailedResponse(ID serverId) {
        replicators.get(serverId).logFailedResponse();
    }

    public List<Integer> getFollowerMatchIndices() {
        return cluster.getOtherMemberIds().stream()
                .map(followerId -> replicators.get(followerId))
                .map(LogReplicator::getMatchIndex)
                .collect(toList());
    }
}
