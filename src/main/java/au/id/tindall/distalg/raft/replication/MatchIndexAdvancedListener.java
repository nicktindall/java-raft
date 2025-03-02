package au.id.tindall.distalg.raft.replication;

public interface MatchIndexAdvancedListener<I> {

    void matchIndexAdvanced(I followerId, int newMatchIndex);
}
