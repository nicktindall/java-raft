package au.id.tindall.distalg.raft.replication;

import java.io.Serializable;

public interface MatchIndexAdvancedListener<I extends Serializable> {

    void matchIndexAdvanced(I followerId, int newMatchIndex);
}
