package au.id.tindall.distalg.raft.serverstates.clustermembership;

import au.id.tindall.distalg.raft.cluster.Configuration;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.replication.ReplicationManager;
import au.id.tindall.distalg.raft.rpc.clustermembership.AddServerResponse;
import au.id.tindall.distalg.raft.state.PersistentState;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.function.Supplier;

import static org.apache.logging.log4j.LogManager.getLogger;

class AddServer<ID extends Serializable> extends MembershipChange<ID, AddServerResponse> {

    private static final Logger LOGGER = getLogger();

    private final int numberOfCatchUpRounds;
    private ReplicationCatchUpRound currentRound;

    AddServer(Log log, Configuration<ID> configuration, PersistentState<ID> persistentState,
              ReplicationManager<ID> replicationManager, ID serverId,
              int numberOfCatchupRounds, Supplier<Instant> timeSource) {
        super(log, configuration, persistentState, replicationManager, serverId, timeSource);
        this.numberOfCatchUpRounds = numberOfCatchupRounds;
        currentRound = new ReplicationCatchUpRound(
                1,
                timeSource.get(),
                log.getLastLogIndex());
    }

    @Override
    protected void onStart() {
        replicationManager.startReplicatingTo(serverId);
    }

    @Override
    protected AddServerResponse matchIndexAdvancedInternal(int lastAppendedIndex) {
        if (currentRound.isFinishedAtIndex(lastAppendedIndex)) {
            if (currentRound.isLast()) {
                if (currentRound.finishedInTime()) {
                    finishedAtIndex = addServerToConfig(serverId);
                    return null;
                } else {
                    replicationManager.stopReplicatingTo(serverId);
                    LOGGER.debug("Catch up round took too long, timing out");
                    return AddServerResponse.TIMEOUT;
                }
            } else {
                currentRound = currentRound.next();
            }
        }
        return null;
    }

    @Override
    protected AddServerResponse timeoutIfSlow() {
        final Duration newServerTimeout = configuration.getElectionTimeout().multipliedBy(3);
        if (finishedAtIndex == NOT_SET
                && lastProgressTime != null
                && lastProgressTime.plus(newServerTimeout).isBefore(timeSource.get())) {
            replicationManager.stopReplicatingTo(serverId);
            LOGGER.debug("No response from server in {}, timing out", newServerTimeout);
            return AddServerResponse.TIMEOUT;
        }
        return null;
    }

    @Override
    protected AddServerResponse entryCommittedInternal(int index) {
        if (finishedAtIndex != NOT_SET && finishedAtIndex <= index) {
            return AddServerResponse.OK;
        }
        return null;
    }

    @Override
    public void close() {
        responseFuture.complete(AddServerResponse.NOT_LEADER);
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
    }
}
