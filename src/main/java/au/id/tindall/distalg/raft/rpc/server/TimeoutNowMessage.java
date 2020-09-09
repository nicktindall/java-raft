package au.id.tindall.distalg.raft.rpc.server;

import au.id.tindall.distalg.raft.log.Term;

import java.io.Serializable;

public class TimeoutNowMessage<ID extends Serializable> extends UnicastMessage<ID> {

    public TimeoutNowMessage(Term term, ID source) {
        super(term, source, source);
    }

    public TimeoutNowMessage(Term term, ID source, ID destination) {
        super(term, source, destination);
    }
}
