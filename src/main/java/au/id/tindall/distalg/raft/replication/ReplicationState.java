package au.id.tindall.distalg.raft.replication;

import org.apache.logging.log4j.Logger;

import java.io.Serializable;

import static org.apache.logging.log4j.LogManager.getLogger;

public class ReplicationState<I extends Serializable> {

    private static final Logger LOGGER = getLogger();

    private final I followerId;
    private final MatchIndexAdvancedListener<I> matchIndexAdvancedListener;
    private volatile int matchIndex;
    private volatile int nextIndex;

    public ReplicationState(I followerId, int nextLogIndex, MatchIndexAdvancedListener<I> matchIndexAdvancedListener) {
        this.followerId = followerId;
        this.matchIndex = 0;
        this.nextIndex = nextLogIndex;
        this.matchIndexAdvancedListener = matchIndexAdvancedListener;
    }

    public I getFollowerId() {
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
        oldMatchIndex = this.matchIndex;
        this.matchIndex = matchIndex;
        this.nextIndex = nextIndex;
        if (oldMatchIndex < matchIndex) {
            matchIndexAdvancedListener.matchIndexAdvanced(followerId, matchIndex);
        }
    }

    public void logSuccessResponse(int lastAppendedIndex) {
        int oldMatchIndex;
        int newMatchIndex;
        oldMatchIndex = matchIndex;
        nextIndex = Math.max(lastAppendedIndex + 1, nextIndex);
        matchIndex = newMatchIndex = Math.max(lastAppendedIndex, matchIndex);
        if (oldMatchIndex < newMatchIndex) {
            matchIndexAdvancedListener.matchIndexAdvanced(followerId, newMatchIndex);
        }
    }

    public void logFailedResponse(Integer earliestPossibleMatchIndex) {
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
