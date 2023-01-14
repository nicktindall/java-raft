package au.id.tindall.distalg.raft.util;

public enum ThreadUtil {
    ;

    /**
     * Pause, restoring interrupt if interrupted
     *
     * @param milliseconds milliseconds to pause for
     */
    public static void pauseMillis(long milliseconds) {
        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Pause, restoring interrupt if interrupted
     *
     * @param microseconds microseconds to pause for
     */
    public static void pauseMicros(int microseconds) {
        try {
            Thread.sleep(microseconds / 1_000, (microseconds % 1_000) * 1_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
