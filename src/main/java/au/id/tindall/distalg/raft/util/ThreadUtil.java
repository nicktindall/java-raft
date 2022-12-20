package au.id.tindall.distalg.raft.util;

public enum ThreadUtil {
    ;

    /**
     * Pause, restoring interrupt if interrupted
     *
     * @param milliseconds
     */
    public static void pause(long milliseconds) {
        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
