package au.id.tindall.distalg.raft;

import au.id.tindall.distalg.raft.client.sessions.ClientSessionStore;
import au.id.tindall.distalg.raft.comms.Inbox;
import au.id.tindall.distalg.raft.comms.MessageProcessor;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.serverstates.ServerStateType;
import au.id.tindall.distalg.raft.statemachine.StateMachine;

import java.io.Closeable;
import java.io.Serializable;
import java.util.Optional;

public interface Server<I extends Serializable> extends MessageProcessor<I>, Closeable {

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

    Inbox<I> getInbox();
}
