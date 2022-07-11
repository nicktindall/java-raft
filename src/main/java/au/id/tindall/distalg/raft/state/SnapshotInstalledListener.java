package au.id.tindall.distalg.raft.state;

public interface SnapshotInstalledListener {

    void onSnapshotInstalled(Snapshot snapshot);
}
