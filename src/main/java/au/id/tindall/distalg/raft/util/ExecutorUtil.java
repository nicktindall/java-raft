package au.id.tindall.distalg.raft.util;

import org.apache.logging.log4j.Logger;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.logging.log4j.LogManager.getLogger;

public enum ExecutorUtil {
    ;
    private static final Logger LOGGER = getLogger();

    /**
     * Shut down an executor service and await termination, warning if it doesn't shut down in time, or if we're
     * interrupted waiting for it to shut down
     *
     * @param executorService       The ExecutorService
     * @param timeoutInMilliseconds The timeout, in milliseconds
     */
    public static void shutdownAndAwaitTermination(ExecutorService executorService, long timeoutInMilliseconds) {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(timeoutInMilliseconds, TimeUnit.MILLISECONDS)) {
                LOGGER.warn("ExecutorService didn't shutdown in {}ms", timeoutInMilliseconds);
            }
        } catch (InterruptedException e) {
            LOGGER.warn("Interrupted waiting for ExecutorService to terminate");
            Thread.currentThread().interrupt();
        }
    }
}
