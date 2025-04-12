package au.id.tindall.distalg.raft.comms;

import au.id.tindall.distalg.raft.clusterclient.ClusterClient;

import java.io.Closeable;

public interface TestInfrastructureFactory<I> extends ClusterFactory<I>, Closeable {

    ClusterClient<I> createClusterClient();
}
