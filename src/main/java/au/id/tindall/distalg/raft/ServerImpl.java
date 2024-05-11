package au.id.tindall.distalg.raft;

import au.id.tindall.distalg.raft.client.sessions.ClientSessionStore;
import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.comms.Inbox;
import au.id.tindall.distalg.raft.elections.ElectionScheduler;
import au.id.tindall.distalg.raft.elections.ElectionTimeoutProcessor;
import au.id.tindall.distalg.raft.exceptions.AlreadyRunningException;
import au.id.tindall.distalg.raft.exceptions.NotRunningException;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.processors.InboxProcessor;
import au.id.tindall.distalg.raft.processors.ProcessorController;
import au.id.tindall.distalg.raft.processors.ProcessorManager;
import au.id.tindall.distalg.raft.processors.RaftProcessorGroup;
import au.id.tindall.distalg.raft.rpc.client.ClientRequestMessage;
import au.id.tindall.distalg.raft.rpc.client.ClientResponseMessage;
import au.id.tindall.distalg.raft.rpc.clustermembership.ServerAdminRequest;
import au.id.tindall.distalg.raft.rpc.clustermembership.ServerAdminResponse;
import au.id.tindall.distalg.raft.rpc.clustermembership.TransferLeadershipRequest;
import au.id.tindall.distalg.raft.rpc.server.RpcMessage;
import au.id.tindall.distalg.raft.rpc.server.TimeoutNowMessage;
import au.id.tindall.distalg.raft.rpc.server.TransferLeadershipMessage;
import au.id.tindall.distalg.raft.serverstates.Result;
import au.id.tindall.distalg.raft.serverstates.ServerState;
import au.id.tindall.distalg.raft.serverstates.ServerStateFactory;
import au.id.tindall.distalg.raft.serverstates.ServerStateType;
import au.id.tindall.distalg.raft.state.PersistentState;
import au.id.tindall.distalg.raft.statemachine.StateMachine;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.io.Serializable;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static au.id.tindall.distalg.raft.util.Closeables.closeQuietly;
import static org.apache.logging.log4j.LogManager.getLogger;

public class ServerImpl<I extends Serializable> implements Server<I>, Closeable {

    private static final Logger LOGGER = getLogger();

    private final PersistentState<I> persistentState;
    private final ServerStateFactory<I> serverStateFactory;
    private final StateMachine stateMachine;
    private final Cluster<I> cluster;
    private final ElectionScheduler electionScheduler;
    private final ProcessorManager<RaftProcessorGroup> processorManager;
    private final Inbox<I> inbox;
    private ServerState<I> state;
    private ProcessorController inboxProcessorController;
    private ProcessorController electionTimeoutProcessorController;

    public ServerImpl(PersistentState<I> persistentState, ServerStateFactory<I> serverStateFactory, StateMachine stateMachine, Cluster<I> cluster,
                      ElectionScheduler electionScheduler, ProcessorManager<RaftProcessorGroup> processorManager, Inbox<I> inbox) {
        this.persistentState = persistentState;
        this.serverStateFactory = serverStateFactory;
        this.stateMachine = stateMachine;
        this.cluster = cluster;
        this.electionScheduler = electionScheduler;
        this.processorManager = processorManager;
        this.inbox = inbox;
    }

    @Override
    public boolean timeoutNowIfDue() {
        if (electionScheduler.shouldTimeout()) {
            LOGGER.debug("Election timeout occurred: server {}", persistentState.getId());
            electionTimeout();
            return true;
        }
        return false;
    }

    @Override
    public synchronized void start() {
        if (inboxProcessorController != null) {
            throw new AlreadyRunningException("Can't start, server is already started!");
        }
        inboxProcessorController = processorManager.runProcessor(new InboxProcessor<>(this, inbox, this::initialise, this::terminate));
        electionTimeoutProcessorController = processorManager.runProcessor(new ElectionTimeoutProcessor<>(this));
    }

    @Override
    public synchronized void stop() {
        if (inboxProcessorController == null) {
            throw new NotRunningException("Can't stop, server is not started");
        }
        inboxProcessorController.stopAndWait();
        inboxProcessorController = null;
        electionTimeoutProcessorController.stopAndWait();
        electionTimeoutProcessorController = null;
    }

    private void initialise() {
        cluster.onStart();
        updateState(serverStateFactory.createInitialState());
    }

    private void terminate() {
        updateState(null);
        cluster.onStop();
    }

    @Override
    public <R extends ClientResponseMessage> CompletableFuture<R> handle(ClientRequestMessage<I, R> clientRequestMessage) {
        assertThatNodeIsRunning();
        return state.handle(clientRequestMessage);
    }

    @Override
    public <R extends ServerAdminResponse> CompletableFuture<R> handle(ServerAdminRequest<R> serverAdminRequest) {
        assertThatNodeIsRunning();
        if (serverAdminRequest instanceof TransferLeadershipRequest) {
            transferLeadership();
            return CompletableFuture.completedFuture(null);
        } else {
            return state.handle(serverAdminRequest);
        }
    }

    @Override
    public void handle(RpcMessage<I> message) {
        assertThatNodeIsRunning();
        Result<I> result;
        do {
            result = state.handle(message);
            updateState(result.getNextState());
        } while (!result.isFinished());
    }

    @Override
    public void initialize() {
        persistentState.initialize();
    }

    private void assertThatNodeIsRunning() {
        if (state == null) {
            throw new NotRunningException("Server is not running, call start() before attempting to interact with it");
        }
    }

    private void updateState(ServerState<I> nextState) {
        if (state != nextState) {
            if (state != null) {
                state.leaveState();
            }
            state = nextState;
            if (nextState != null) {
                state.enterState();
            }
        }
    }

    private void electionTimeout() {
        handle(new TimeoutNowMessage<>(persistentState.getCurrentTerm(), persistentState.getId(), persistentState.getId()));
    }

    @Override
    public void transferLeadership() {
        handle(new TransferLeadershipMessage<>(persistentState.getCurrentTerm(), persistentState.getId()));
    }

    @Override
    public I getId() {
        return persistentState.getId();
    }

    @Override
    public Optional<ServerStateType> getState() {
        return Optional.ofNullable(state)
                .map(ServerState::getServerStateType);
    }

    @Override
    public Log getLog() {
        return state.getLog();
    }

    @Override
    public ClientSessionStore getClientSessionStore() {
        return serverStateFactory.getClientSessionStore();
    }

    @Override
    public StateMachine getStateMachine() {
        return this.stateMachine;
    }

    @Override
    public void close() {
        if (state != null) {
            stop();
        }
        closeQuietly(inbox, serverStateFactory, persistentState, processorManager);
    }

    @Override
    public Inbox<I> getInbox() {
        return inbox;
    }
}
