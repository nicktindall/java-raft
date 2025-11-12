package au.id.tindall.distalg.raft.comms.netty.simplemesh;

import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class NetworkUtilTest {

    @Test
    void testResolve() throws UnknownHostException {
        InetSocketAddress[] resolve = NetworkUtil.resolve("test.localhost:1234");
        assertThat(resolve).isNotEmpty();
        assertThat(resolve).allMatch(addr -> addr.getPort() == 1234);
    }

    @Test
    void testResolveFirst() {
        InetSocketAddress resolve = NetworkUtil.resolveFirst("test.localhost:1234");
        assertThat(resolve.getHostName()).isEqualTo("test.localhost");
        assertThat(resolve.getPort()).isEqualTo(1234);
    }
}