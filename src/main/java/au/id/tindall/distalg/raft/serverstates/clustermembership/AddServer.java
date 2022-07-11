package au.id.tindall.distalg.raft.serverstates.clustermembership;

import au.id.tindall.distalg.raft.cluster.Configuration;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.replication.ReplicationManager;
import au.id.tindall.distalg.raft.rpc.clustermembership.AddServerResponse;
import au.id.tindall.distalg.raft.state.PersistentState;

import java.io.Serializable;
import java.time.Instant;
import java.util.function.Supplier;

class AddServer<ID extends Serializable> extends MembershipChange<ID, AddServerResponse> {

    private static final int DEFAULT_NUMBER_OF_CATCHUP_ROUNDS = 10;

    private final Log log;
    private final ReplicationManager<ID> replicationManager;
    private final int numberOfCatchUpRounds;
    private final Supplier<Instant> timeSource;
    private Instant lastProgressTime;

    private ReplicationCatchUpRound round;

    AddServer(Log log, Configuration<ID> configuration, PersistentState<ID> persistentState,
              ReplicationManager<ID> replicationManager, ID serverId,
              Supplier<Instant> timeSource) {
        this(log, configuration, persistentState, replicationManager, serverId,
                DEFAULT_NUMBER_OF_CATCHUP_ROUNDS, timeSource);
    }

    AddServer(Log log, Configuration<ID> configuration, PersistentState<ID> persistentState,
              ReplicationManager<ID> replicationManager, ID serverId,
              int numberOfCatchupRounds, Supplier<Instant> timeSource) {
        super(log, configuration, persistentState, serverId);
        this.log = log;
        this.replicationManager = replicationManager;
        this.numberOfCatchUpRounds = numberOfCatchupRounds;
        this.timeSource = timeSource;
        round = new ReplicationCatchUpRound(
                1,
                timeSource.get(),
                log.getLastLogIndex());
    }

    @Override
    void start() {
        replicationManager.startReplicatingTo(serverId);
        lastProgressTime = timeSource.get();
    }

    @Override
    AddServerResponse logSuccessResponseInternal(ID serverId, int lastAppendedIndex) {
        if (this.serverId.equals(serverId)) {
            lastProgressTime = timeSource.get();
            if (round.isFinishedAtIndex(lastAppendedIndex)) {
                if (round.isLast()) {
                    if (round.finishedInTime()) {
                        finishedAtIndex = addServerToConfig(serverId);
                        return null;
                    } else {
                        replicationManager.stopReplicatingTo(serverId);
                        return AddServerResponse.TIMEOUT;
                    }
                } else {
                    round = round.next();
                }
            }
        }
        return null;
    }

    @Override
    AddServerResponse logFailureResponseInternal(ID serverId) {
        if (this.serverId.equals(serverId)) {
            lastProgressTime = timeSource.get();
        }
        return null;
    }

    @Override
    protected AddServerResponse timeoutIfSlow() {
        if (finishedAtIndex == -1
                && lastProgressTime != null
                && lastProgressTime.plus(configuration.getElectionTimeout().multipliedBy(3)).isBefore(timeSource.get())) {
            replicationManager.stopReplicatingTo(serverId);
            return AddServerResponse.TIMEOUT;
        }
        return null;
    }

    @Override
    public void logSnapshotResponse(ID serverId) {
        if (this.serverId.equals(serverId)) {
            lastProgressTime = timeSource.get();
        }
    }

    @Override
    protected AddServerResponse entryCommittedInternal(int index) {
        if (finishedAtIndex == index) {
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
