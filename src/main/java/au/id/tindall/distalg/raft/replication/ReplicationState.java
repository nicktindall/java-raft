package au.id.tindall.distalg.raft.replication;

import org.apache.logging.log4j.Logger;

import java.io.Serializable;

import static org.apache.logging.log4j.LogManager.getLogger;

public class ReplicationState<ID extends Serializable> {

    private static final Logger LOGGER = getLogger();

    private final ID followerId;
    private volatile int matchIndex;
    private volatile int nextIndex;

    public ReplicationState(ID followerId, int nextLogIndex) {
        this.followerId = followerId;
        this.matchIndex = 0;
        this.nextIndex = nextLogIndex;
    }

    public ID getFollowerId() {
        return followerId;
    }

    public int getMatchIndex() {
        return matchIndex;
    }

    public int getNextIndex() {
        return nextIndex;
    }

    public synchronized void updateIndices(int matchIndex, int nextIndex) {
        this.matchIndex = matchIndex;
        this.nextIndex = nextIndex;
    }

    public synchronized void logSuccessResponse(int lastAppendedIndex) {
        nextIndex = Math.max(lastAppendedIndex + 1, nextIndex);
        matchIndex = Math.max(lastAppendedIndex, matchIndex);
    }

    public synchronized void logFailedResponse(Integer earliestPossibleMatchIndex) {
        LOGGER.debug("Got failed response from {}, earliest possible match index: {}", followerId, earliestPossibleMatchIndex);
        if (earliestPossibleMatchIndex != null) {
            nextIndex = earliestPossibleMatchIndex;
        }
    }

    @Override
    public String toString() {
        return "ReplicationState{" +
                "matchIndex=" + matchIndex +
                ", nextIndex=" + nextIndex +
                '}';
    }
}
