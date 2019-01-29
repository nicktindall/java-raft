package au.id.tindall.distalg.raft;

import static au.id.tindall.distalg.raft.ServerState.FOLLOWER;

import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.Term;

public class Server<ID> {

    private final ID id;
    private Term currentTerm;
    private ServerState state;
    private ID votedFor;
    private Log log;

    public Server(ID id) {
        this(id, new Term(0), null, new Log());
    }

    public Server(ID id, Term currentTerm, ID votedFor, Log log) {
        this.id = id;
        this.currentTerm = currentTerm;
        this.votedFor = votedFor;
        this.log = log;
        state = FOLLOWER;
    }

    public ID getId() {
        return id;
    }

    public Term getCurrentTerm() {
        return currentTerm;
    }

    public ServerState getState() {
        return state;
    }

    public ID getVotedFor() {
        return votedFor;
    }

    public Log getLog() {
        return log;
    }
}
