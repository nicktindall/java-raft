package au.id.tindall.distalg.raft.util;

import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;

import static org.apache.logging.log4j.LogManager.getLogger;

class ThreadUtilTest {

    private static final Logger LOGGER = getLogger();

    @SuppressWarnings("java:S2699")
    @Test
    void pauseMicros() {
        for (int i = 0; i < 30; i++) {
            long startTime = System.nanoTime();
            final int requestedMicroseconds = i * 500;
            ThreadUtil.pauseMicros(requestedMicroseconds);
            long durationNanos = System.nanoTime() - startTime;
            LOGGER.info("Sleep for {}us slept for {}us", requestedMicroseconds, durationNanos / 1_000);
        }
    }

    @SuppressWarnings("java:S2699")
    @Test
    void pauseMillis() {
        for (int i = 0; i < 30; i++) {
            long startTime = System.nanoTime();
            final int requestedMilliseconds = i;
            ThreadUtil.pauseMillis(requestedMilliseconds);
            long durationNanos = System.nanoTime() - startTime;
            LOGGER.info("Sleep for {}ms slept for {}ms", requestedMilliseconds, durationNanos / 1_000_000);
        }
    }
}