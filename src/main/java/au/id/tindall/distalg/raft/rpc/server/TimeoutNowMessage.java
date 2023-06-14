package au.id.tindall.distalg.raft.rpc.server;

import au.id.tindall.distalg.raft.log.Term;

import java.io.Serializable;

public class TimeoutNowMessage<I extends Serializable> extends UnicastMessage<I> {

    public TimeoutNowMessage(Term term, I source) {
        super(term, source, source);
    }

    public TimeoutNowMessage(Term term, I source, I destination) {
        super(term, source, destination);
    }
}
