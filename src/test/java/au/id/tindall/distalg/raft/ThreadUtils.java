package au.id.tindall.distalg.raft;

public class ThreadUtils {

    /**
     * Pause with a runtime exception on interrupt
     *
     * @param milliseconds
     */
    public static void pause(long milliseconds) {
        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted pausing", e);
        }
    }
}
