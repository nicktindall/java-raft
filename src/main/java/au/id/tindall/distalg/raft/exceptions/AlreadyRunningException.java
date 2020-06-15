package au.id.tindall.distalg.raft.exceptions;

public class AlreadyRunningException extends IllegalStateException {

    public AlreadyRunningException(String s) {
        super(s);
    }
}
