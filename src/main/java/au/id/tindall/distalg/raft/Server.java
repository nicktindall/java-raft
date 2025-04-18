package au.id.tindall.distalg.raft;

import au.id.tindall.distalg.raft.client.sessions.ClientSessionStore;
import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.comms.MessageProcessor;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.serverstates.ServerStateType;
import au.id.tindall.distalg.raft.statemachine.StateMachine;

import java.io.Closeable;
import java.util.Optional;

public interface Server<I> extends MessageProcessor<I>, Closeable {

    boolean timeoutNowIfDue();

    void start();

    void stop();

    void initialize();

    void transferLeadership();

    I getId();

    Optional<ServerStateType> getState();

    Log getLog();

    ClientSessionStore getClientSessionStore();

    StateMachine getStateMachine();

    @Override
    void close();

    Cluster<I> getCluster();

    Term getTerm();
}
