package au.id.tindall.distalg.raft.serverstates.leadershiptransfer;

import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.replication.LogReplicator;
import au.id.tindall.distalg.raft.state.PersistentState;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.apache.logging.log4j.LogManager.getLogger;

public class LeadershipTransfer<ID extends Serializable> {
    private static final Logger LOGGER = getLogger();

    private static final Duration MINIMUM_INTERVAL_BETWEEN_TIMEOUT_NOW_MESSAGES = Duration.ofMillis(100);
    private static final Duration INDIVIDUAL_TARGET_TIMEOUT = Duration.ofMillis(1000);
    private static final Duration LEADERSHIP_TRANSFER_TIMEOUT = Duration.ofMillis(5000);

    private final Map<ID, LogReplicator<ID>> replicators;
    private final Cluster<ID> cluster;
    private final PersistentState<ID> persistentState;
    private final Supplier<Long> currentTimeProvider;
    private Instant transferStartTime;

    private TransferTarget transferTarget;

    public LeadershipTransfer(Cluster<ID> cluster, PersistentState<ID> persistentState, Map<ID, LogReplicator<ID>> replicators) {
        this(cluster, persistentState, replicators, System::currentTimeMillis);
    }

    public LeadershipTransfer(Cluster<ID> cluster, PersistentState<ID> persistentState, Map<ID, LogReplicator<ID>> replicators, Supplier<Long> currentTimeProvider) {
        this.cluster = cluster;
        this.replicators = replicators;
        this.persistentState = persistentState;
        this.currentTimeProvider = currentTimeProvider;
    }

    public boolean isInProgress() {
        abortTransferIfTimeoutHasBeenExceeded();
        return transferTarget != null;
    }

    public void start() {
        if (isInProgress()) {
            return;
        }
        transferStartTime = currentInstant();
        selectLeadershipTransferTarget(emptySet());
        sendTimeoutNowRequestIfReadyToTransfer();
    }

    public void sendTimeoutNowRequestIfReadyToTransfer() {
        if (!isInProgress()) {
            return;
        }
        selectNewTargetIfTimeoutHasBeenExceededAndThereAreOthersAvailable();
        if (transferTarget.isUpToDate()
                && transferTarget.minimumIntervalBetweenTimeoutNowMessagesHasPassed()) {
            cluster.sendTimeoutNowRequest(persistentState.getCurrentTerm(), transferTarget.id);
            transferTarget.lastTimeoutMessageSent = currentInstant();
        }
    }

    private void selectNewTargetIfTimeoutHasBeenExceededAndThereAreOthersAvailable() {
        if (transferTarget.hasTimedOut()) {
            if (replicators.size() > 1) {
                selectLeadershipTransferTarget(singleton(transferTarget.id));
            }
        }
    }

    private void abortTransferIfTimeoutHasBeenExceeded() {
        if (transferStartTime != null && transferStartTime.plus(LEADERSHIP_TRANSFER_TIMEOUT).isBefore(currentInstant())) {
            LOGGER.debug("Leadership transfer timeout exceeded, aborting");
            transferTarget = null;
            transferStartTime = null;
        }
    }

    private void selectLeadershipTransferTarget(Set<ID> excludedIds) {
        ID targetId = replicators.entrySet().stream()
                .filter(replicator -> !excludedIds.contains(replicator.getKey()))
                .min((replicator1, replicator2) -> replicator2.getValue().getMatchIndex() - replicator1.getValue().getMatchIndex())
                .orElseThrow(() -> new IllegalStateException("No followers to transfer to!"))
                .getKey();
        transferTarget = new TransferTarget(targetId);
    }

    private Instant currentInstant() {
        return Instant.ofEpochMilli(currentTimeProvider.get());
    }

    private class TransferTarget {
        private final ID id;
        private final Instant selectedAt;
        private Instant lastTimeoutMessageSent;

        private TransferTarget(ID id) {
            this.id = id;
            this.selectedAt = currentInstant();
        }

        public boolean hasTimedOut() {
            return selectedAt.plus(INDIVIDUAL_TARGET_TIMEOUT).isBefore(currentInstant());
        }

        private boolean minimumIntervalBetweenTimeoutNowMessagesHasPassed() {
            return lastTimeoutMessageSent == null || lastTimeoutMessageSent.plus(MINIMUM_INTERVAL_BETWEEN_TIMEOUT_NOW_MESSAGES).isBefore(currentInstant());
        }

        public boolean isUpToDate() {
            return replicators.get(id).getMatchIndex() == persistentState.getLogStorage().getLastLogIndex();
        }
    }
}
