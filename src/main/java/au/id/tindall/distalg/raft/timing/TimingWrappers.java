package au.id.tindall.distalg.raft.timing;

import au.id.tindall.distalg.raft.Server;
import au.id.tindall.distalg.raft.serverstates.Result;
import au.id.tindall.distalg.raft.serverstates.ServerState;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.logging.log4j.LogManager.getLogger;

public enum TimingWrappers {
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
                LOGGER.warn("Server#{} took {}ms (expected < {}ms, server={})", callIdentifier, tookMillis, warningThresholdMillis, delegate.getId());
            }
            if (t != null) {
                throw t;
            }
            return returnVal;
        });
    }

    /**
     * Wrap a ServerState in a timing proxy
     *
     * @param delegate               The ServerState to wrap
     * @param warningThresholdMillis The threshold over which to warn (MS)
     * @param <ID>                   The ID type
     * @return A proxy to the server that will time method calls and warn when they exceed
     */
    @SuppressWarnings("unchecked")
    public static <ID extends Serializable> ServerState<ID> wrap(ServerState<ID> delegate, long warningThresholdMillis) {
        final AtomicReference<ServerState<ID>> theProxy = new AtomicReference<>();
        final ServerState<ID> serverStateProxy = (ServerState<ID>) Proxy.newProxyInstance(Server.class.getClassLoader(), new Class[]{ServerState.class}, (proxy, method, args) -> {
            long startTime = System.currentTimeMillis();
            Throwable t = null;
            Object returnVal = null;
            try {
                returnVal = method.invoke(delegate, args);
                if (returnVal instanceof Result) {
                    final Result<?> result = (Result<?>) returnVal;
                    if (result.getNextState() == delegate) {
                        returnVal = result.isFinished() ? Result.complete(theProxy.get()) : Result.incomplete(theProxy.get());
                    }

                }
            } catch (InvocationTargetException e) {
                t = e.getCause();
            }
            long tookMillis = System.currentTimeMillis() - startTime;
            if (tookMillis > warningThresholdMillis) {
                final String methodName = delegate.getClass().getSimpleName() + "#" + method.getName();
                final String callIdentifier = method.getName().equals("handle") ? methodName + "(" + args[0].getClass().getSimpleName() + ")" : methodName;
                LOGGER.warn("{} took {}ms (expected < {}ms)", callIdentifier, tookMillis, warningThresholdMillis);
            }
            if (t != null) {
                throw t;
            }
            return returnVal;
        });
        theProxy.set(serverStateProxy);
        return serverStateProxy;
    }
}
