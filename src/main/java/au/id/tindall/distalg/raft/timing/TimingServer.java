package au.id.tindall.distalg.raft.timing;

import au.id.tindall.distalg.raft.Server;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import static org.apache.logging.log4j.LogManager.getLogger;

public enum TimingServer {
    ;

    private static final Logger LOGGER = getLogger();

    /**
     * Wrap a Server in a timing proxy
     *
     * @param delegate               The server to wrap
     * @param warningThresholdMillis The threshold over which to warn (MS)
     * @param <ID>                   The ID type
     * @return A proxy to the server that will time method calls and warn when they exceed
     */
    @SuppressWarnings("unchecked")
    public static <ID extends Serializable> Server<ID> wrap(Server<ID> delegate, long warningThresholdMillis) {
        return (Server<ID>) Proxy.newProxyInstance(Server.class.getClassLoader(), new Class[]{Server.class}, (Object proxy, Method method, Object[] args) -> {
            long startTime = System.currentTimeMillis();
            Throwable t = null;
            Object returnVal = null;
            try {
                returnVal = method.invoke(delegate, args);
            } catch (InvocationTargetException e) {
                t = e.getCause();
            }
            long tookMillis = System.currentTimeMillis() - startTime;
            if (tookMillis > warningThresholdMillis) {
                final String callIdentifier = method.getName().equals("handle") ? method.getName() + "(" + args[0].getClass().getSimpleName() + ")" : method.getName();
                LOGGER.warn("{} took {}ms (expected < {}ms, server={})", callIdentifier, tookMillis, warningThresholdMillis, delegate.getId());
            }
            if (t != null) {
                throw t;
            }
            return returnVal;
        });
    }
}
