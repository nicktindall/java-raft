package au.id.tindall.distalg.raft.rpc.server;

import au.id.tindall.distalg.raft.log.Term;

import java.io.Serializable;

public class TimeoutNowMessage<I extends Serializable> extends UnicastMessage<I> {

    private final boolean earlyElection;

    public TimeoutNowMessage(Term term, I source) {
        this(term, source, source, false);
    }

    public TimeoutNowMessage(Term term, I source, I destination, boolean earlyElection) {
        super(term, source, destination);
        this.earlyElection = earlyElection;
    }

    public boolean isEarlyElection() {
        return earlyElection;
    }
}
