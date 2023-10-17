package au.id.tindall.distalg.raft.serverstates.leadershiptransfer;

import au.id.tindall.distalg.raft.cluster.Configuration;
import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.replication.ReplicationManager;
import au.id.tindall.distalg.raft.state.PersistentState;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.Set;
import java.util.function.Supplier;

import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.apache.logging.log4j.LogManager.getLogger;

public class LeadershipTransfer<I extends Serializable> {
    private static final Logger LOGGER = getLogger();

    private static final Duration MINIMUM_INTERVAL_BETWEEN_TIMEOUT_NOW_MESSAGES = Duration.ofMillis(100);
    private static final Duration INDIVIDUAL_TARGET_TIMEOUT = Duration.ofMillis(1000);
    private static final Duration LEADERSHIP_TRANSFER_TIMEOUT = Duration.ofMillis(5000);

    private final ReplicationManager<I> replicationManager;
    private final Cluster<I> cluster;
    private final PersistentState<I> persistentState;
    private final Supplier<Long> currentTimeProvider;
    private final Configuration<I> configuration;
    private Instant transferStartTime;

    private TransferTarget transferTarget;

    public LeadershipTransfer(Cluster<I> cluster, PersistentState<I> persistentState, ReplicationManager<I> replicationManager, Configuration<I> configuration) {
        this(cluster, persistentState, replicationManager, configuration, System::currentTimeMillis);
    }

    public LeadershipTransfer(Cluster<I> cluster, PersistentState<I> persistentState, ReplicationManager<I> replicationManager, Configuration<I> configuration, Supplier<Long> currentTimeProvider) {
        this.cluster = cluster;
        this.replicationManager = replicationManager;
        this.persistentState = persistentState;
        this.currentTimeProvider = currentTimeProvider;
        this.configuration = configuration;
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
            LOGGER.debug("Telling {} to timeout now", transferTarget);
            cluster.sendTimeoutNowRequest(persistentState.getCurrentTerm(), transferTarget.id);
            transferTarget.lastTimeoutMessageSent = currentInstant();
        }
    }

    private void selectNewTargetIfTimeoutHasBeenExceededAndThereAreOthersAvailable() {
        if (transferTarget.hasTimedOut()) {
            LOGGER.debug("Leadership transfer to {} has timed out, selecting new target", transferTarget);
            if (configuration.getOtherServerIds().size() > 1) {
                selectLeadershipTransferTarget(singleton(transferTarget.id));
            }
        }
    }

    private void abortTransferIfTimeoutHasBeenExceeded() {
        if (transferStartTime != null && transferStartTime.plus(LEADERSHIP_TRANSFER_TIMEOUT).isBefore(currentInstant())) {
            LOGGER.warn("Leadership transfer timeout exceeded, aborting");
            transferTarget = null;
            transferStartTime = null;
        }
    }

    private void selectLeadershipTransferTarget(Set<I> excludedIds) {
        I targetId = configuration.getOtherServerIds().stream()
                .filter(serverId -> !excludedIds.contains(serverId))
                .min((replicator1, replicator2) -> replicationManager.getMatchIndex(replicator2) - replicationManager.getMatchIndex(replicator1))
                .orElseThrow(() -> new IllegalStateException("No followers to transfer to!"));
        transferTarget = new TransferTarget(targetId);
        LOGGER.debug("Selected {} as new leadership transfer target", transferTarget);
    }

    private Instant currentInstant() {
        return Instant.ofEpochMilli(currentTimeProvider.get());
    }

    private class TransferTarget {
        private final I id;
        private final Instant selectedAt;
        private Instant lastTimeoutMessageSent;

        private TransferTarget(I id) {
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
            return replicationManager.getMatchIndex(id) == persistentState.getLogStorage().getLastLogIndex();
        }

        @Override
        public String toString() {
            return "TransferTarget{" +
                    "id=" + id +
                    ", selectedAt=" + selectedAt +
                    ", lastTimeoutMessageSent=" + lastTimeoutMessageSent +
                    '}';
        }
    }
}
