package au.id.tindall.distalg.raft.util;

public enum ExceptionUtil {
    ;

    public static void rethrowErrors(Throwable t) {
        if (t instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        }
        if (t instanceof Error e) {
            throw e;
        }
    }
}
