package au.id.tindall.distalg.raft.util;

import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.util.Collection;

import static org.apache.logging.log4j.LogManager.getLogger;

public final class Closeables {

    private static final Logger LOGGER = getLogger();

    public static void closeQuietly(Object... closeables) {
        for (Object closeable : closeables) {
            try {
                if (closeable instanceof Collection) {
                    closeQuietly((Collection<?>) closeable);
                }
                if (closeable instanceof Closeable) {
                    ((Closeable) closeable).close();
                }
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
            collection.removeIf(item -> true);
        } catch (UnsupportedOperationException e) {
            // Do nothing, collection may be immutable
        }
    }
}
