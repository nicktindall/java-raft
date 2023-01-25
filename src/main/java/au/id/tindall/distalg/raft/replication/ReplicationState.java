package au.id.tindall.distalg.raft.replication;

import org.apache.logging.log4j.Logger;

import java.io.Serializable;

import static org.apache.logging.log4j.LogManager.getLogger;

public class ReplicationState<ID extends Serializable> {

    private static final Logger LOGGER = getLogger();

    private final ID followerId;
    private final MatchIndexAdvancedListener<ID> matchIndexAdvancedListener;
    private volatile int matchIndex;
    private volatile int nextIndex;

    public ReplicationState(ID followerId, int nextLogIndex, MatchIndexAdvancedListener<ID> matchIndexAdvancedListener) {
        this.followerId = followerId;
        this.matchIndex = 0;
        this.nextIndex = nextLogIndex;
        this.matchIndexAdvancedListener = matchIndexAdvancedListener;
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

    public void updateIndices(int matchIndex, int nextIndex) {
        int oldMatchIndex;
        synchronized (this) {
            oldMatchIndex = this.matchIndex;
            this.matchIndex = matchIndex;
            this.nextIndex = nextIndex;
        }
        if (oldMatchIndex < matchIndex) {
            matchIndexAdvancedListener.matchIndexAdvanced(followerId, matchIndex);
        }
    }

    public void logSuccessResponse(int lastAppendedIndex) {
        int oldMatchIndex;
        int newMatchIndex;
        synchronized (this) {
            oldMatchIndex = matchIndex;
            nextIndex = Math.max(lastAppendedIndex + 1, nextIndex);
            matchIndex = newMatchIndex = Math.max(lastAppendedIndex, matchIndex);
        }
        if (oldMatchIndex < newMatchIndex) {
            matchIndexAdvancedListener.matchIndexAdvanced(followerId, newMatchIndex);
        }
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
