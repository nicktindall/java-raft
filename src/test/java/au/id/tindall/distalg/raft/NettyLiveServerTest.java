package au.id.tindall.distalg.raft;

import au.id.tindall.distalg.raft.comms.TestInfrastructureFactory;
import org.junit.jupiter.api.BeforeAll;

public class NettyLiveServerTest extends LiveServerTest {

    private static NettyTestClusterFactory clusterFactory;

    @BeforeAll
    static void beforeAll() {
        clusterFactory = new NettyTestClusterFactory();
    }

    @Override
    protected TestInfrastructureFactory<Integer> getInfrastructureFactory() {
        return clusterFactory;
    }

    public static void main(String[] args) {
        longRunTest(NettyLiveServerTest.class);
    }
}
