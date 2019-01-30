package au.id.tindall.distalg.raft;

import static au.id.tindall.distalg.raft.ServerState.CANDIDATE;
import static au.id.tindall.distalg.raft.ServerState.FOLLOWER;

import java.io.Serializable;

import au.id.tindall.distalg.raft.comms.MessageDispatcher;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.rpc.RequestVoteRequest;

public class Server<ID extends Serializable> {

    private final ID id;
    private final MessageDispatcher<ID> messageDispatcher;
    private Term currentTerm;
    private ServerState state;
    private ID votedFor;
    private Log log;

    public Server(ID id, MessageDispatcher<ID> messageDispatcher) {
        this(id, new Term(0), null, new Log(), messageDispatcher);
    }

    public Server(ID id, Term currentTerm, ID votedFor, Log log, MessageDispatcher<ID> messageDispatcher) {
        this.id = id;
        this.currentTerm = currentTerm;
        this.votedFor = votedFor;
        this.log = log;
        this.messageDispatcher = messageDispatcher;
        state = FOLLOWER;
    }

    public void electionTimeout() {
        state = CANDIDATE;
        currentTerm = currentTerm.next();
        votedFor = id;
        messageDispatcher.broadcastMessage(new RequestVoteRequest<>(currentTerm, id, log.getLastLogIndex(), log.getLastLogTerm()));
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
