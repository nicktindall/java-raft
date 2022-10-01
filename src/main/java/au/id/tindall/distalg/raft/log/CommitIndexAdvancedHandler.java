package au.id.tindall.distalg.raft.log;

public interface CommitIndexAdvancedHandler {

    void commitIndexAdvanced(int newCommitIndex);
}
