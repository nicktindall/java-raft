package au.id.tindall.distalg.raft;

import au.id.tindall.distalg.raft.client.PendingResponseRegistryFactory;
import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.comms.ClusterFactory;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.LogFactory;
import au.id.tindall.distalg.raft.replication.LogReplicatorFactory;
import au.id.tindall.distalg.raft.serverstates.ServerStateFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ServerFactoryTest {

    private static final Long SERVER_ID = 12345L;

    @Mock
    private ClusterFactory<Long> clusterFactory;
    @Mock
    private Cluster<Long> cluster;
    @Mock
    private LogFactory logFactory;
    @Mock
    private Log log;
    @Mock
    private PendingResponseRegistryFactory pendingResponseRegistryFactory;
    @Mock
    private LogReplicatorFactory<Long> logReplicatorFactory;
    private ServerFactory<Long> serverFactory;

    @BeforeEach
    void setUp() {
        when(clusterFactory.createForNode(eq(SERVER_ID))).thenReturn(cluster);
        when(logFactory.createLog()).thenReturn(log);
        serverFactory = new ServerFactory<>(clusterFactory, logFactory, pendingResponseRegistryFactory, logReplicatorFactory);
    }

    @Test
    void createsServersAndTheirDependencies() {
        assertThat(serverFactory.create(SERVER_ID)).isEqualToComparingFieldByFieldRecursively(new Server<>(SERVER_ID, new ServerStateFactory<>(SERVER_ID,
                log, cluster, pendingResponseRegistryFactory, logReplicatorFactory)));
    }
}