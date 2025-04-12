package au.id.tindall.distalg.raft.serverstates.clustermembership;

import au.id.tindall.distalg.raft.cluster.Configuration;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.replication.ReplicationManager;
import au.id.tindall.distalg.raft.rpc.clustermembership.AddServerResponse;
import au.id.tindall.distalg.raft.state.PersistentState;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.time.Instant;
import java.util.function.Supplier;

import static au.id.tindall.distalg.raft.util.TimestampUtil.formatTimestamp;
import static org.apache.logging.log4j.LogManager.getLogger;

class AddServer<I> extends MembershipChange<I, AddServerResponse<I>> {

    private static final Logger LOGGER = getLogger();

    private final int numberOfCatchUpRounds;
    private ReplicationCatchUpRound currentRound;

    AddServer(Log log, Configuration<I> configuration, PersistentState<I> persistentState,
              ReplicationManager<I> replicationManager, I serverId,
              int numberOfCatchupRounds, Supplier<Instant> timeSource) {
        super(log, configuration, persistentState, replicationManager, serverId, timeSource);
        this.numberOfCatchUpRounds = numberOfCatchupRounds;
        currentRound = new ReplicationCatchUpRound(
                1,
                timeSource.get(),
                log.getLastLogIndex());
        LOGGER.debug("Catching up new server round={}", currentRound);
    }

    @Override
    protected void onStart() {
        replicationManager.startReplicatingTo(serverId);
    }

    @Override
    protected AddServerResponse<I> matchIndexAdvancedInternal(int lastAppendedIndex) {
        if (currentRound.isFinishedAtIndex(lastAppendedIndex)) {
            if (currentRound.isLast()) {
                if (currentRound.finishedInTime()) {
                    LOGGER.debug("Final catch up round finished, completing change");
                    finishedAtIndex = addServerToConfig(serverId);
                    return null;
                } else {
                    replicationManager.stopReplicatingTo(serverId);
                    LOGGER.debug("Catch up round took too long, timing out round={}", currentRound);
                    return AddServerResponse.getTimeout();
                }
            } else {
                ReplicationCatchUpRound lastRound = currentRound;
                currentRound = currentRound.next();
                LOGGER.debug("Catch up round finished, starting next last={}, current={}", lastRound, currentRound);
            }
        }
        return null;
    }

    @Override
    protected AddServerResponse<I> timeoutIfSlow() {
        final Duration newServerTimeout = configuration.getElectionTimeout().multipliedBy(3);
        if (finishedAtIndex == NOT_SET
                && lastProgressTime != null
                && lastProgressTime.plus(newServerTimeout).isBefore(timeSource.get())) {
            replicationManager.stopReplicatingTo(serverId);
            LOGGER.debug("No response from server {} in {}, timing out", serverId, newServerTimeout);
            return AddServerResponse.getTimeout();
        }
        return null;
    }

    @Override
    protected AddServerResponse<I> entryCommittedInternal(int index) {
        if (finishedAtIndex != NOT_SET && finishedAtIndex <= index) {
            return AddServerResponse.getOK();
        }
        return null;
    }

    @Override
    public void close() {
        responseFuture.complete(AddServerResponse.getNotLeader());
    }

    class ReplicationCatchUpRound {
        private final int number;
        private final Instant startTime;
        private final int endIndex;

        public ReplicationCatchUpRound(int number, Instant startTime, int endIndex) {
            this.number = number;
            this.startTime = startTime;
            this.endIndex = endIndex;
        }

        public boolean finishedInTime() {
            return startTime.plus(configuration.getElectionTimeout()).isAfter(timeSource.get());
        }

        public boolean isFinishedAtIndex(int index) {
            return endIndex <= index;
        }

        public boolean isLast() {
            return number == numberOfCatchUpRounds;
        }

        public ReplicationCatchUpRound next() {
            return new ReplicationCatchUpRound(number + 1, timeSource.get(), log.getLastLogIndex());
        }

        @Override
        public String toString() {
            return "ReplicationCatchUpRound{" +
                    "number=" + number +
                    ", startTime=" + formatTimestamp(startTime) +
                    ", endIndex=" + endIndex +
                    '}';
        }
    }
}
