package au.id.tindall.distalg.raft.comms.netty.simplemesh;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public enum NetworkUtil {
    ;
    private static final Pattern IPV4_PATTERN = Pattern.compile("^([^:]+):(\\d{1,5})$");

    public static InetSocketAddress[] resolve(String socketAddressString) throws UnknownHostException {
        Matcher matcher = IPV4_PATTERN.matcher(socketAddressString);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid socket address: " + socketAddressString);
        } else {
            return Arrays.stream(InetAddress.getAllByName(matcher.group(1)))
                    .map(addr -> new InetSocketAddress(addr, Integer.parseInt(matcher.group(2))))
                    .toArray(InetSocketAddress[]::new);
        }
    }

    public static InetSocketAddress resolveFirst(String socketAddressString) {
        Matcher matcher = IPV4_PATTERN.matcher(socketAddressString);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid socket address: " + socketAddressString);
        } else {
            return new InetSocketAddress(matcher.group(1), Integer.parseInt(matcher.group(2)));
        }
    }
}
