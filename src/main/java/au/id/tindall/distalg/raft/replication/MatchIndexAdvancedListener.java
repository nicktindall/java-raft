package au.id.tindall.distalg.raft.replication;

import java.io.Serializable;

public interface MatchIndexAdvancedListener<ID extends Serializable> {

    void matchIndexAdvanced(ID followerId, int newMatchIndex);
}
