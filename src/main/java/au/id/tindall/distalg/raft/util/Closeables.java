package au.id.tindall.distalg.raft.util;

import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.Map;

import static org.apache.logging.log4j.LogManager.getLogger;

public enum Closeables {
    ;

    private static final Logger LOGGER = getLogger();

    public static void closeQuietly(Object... closeables) {
        for (Object closeable : closeables) {
            try {
                if (closeable instanceof Collection) {
                    closeQuietly((Collection<?>) closeable);
                }
                if (closeable instanceof Map) {
                    final Map<?, ?> map = (Map<?, ?>) closeable;
                    map.forEach(Closeables::closeQuietly);
                    map.clear();
                }
                if (closeable instanceof AutoCloseable) {
                    ((AutoCloseable) closeable).close();
                }
            } catch (InterruptedException e) {
                LOGGER.warn("Interrupted closing", e);
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                LOGGER.warn("Error closing", e);
            }
        }
    }

    private static void closeQuietly(Collection<?> collection) {
        for (Object item : collection) {
            closeQuietly(item);
        }
        try {
            collection.clear();
        } catch (UnsupportedOperationException e) {
            // Do nothing, collection may be immutable
        }
    }
}
