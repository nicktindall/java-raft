package au.id.tindall.distalg.raft;

import au.id.tindall.distalg.raft.client.sessions.ClientSessionStore;
import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.driver.NoOpServerDriver;
import au.id.tindall.distalg.raft.driver.ServerDriver;
import au.id.tindall.distalg.raft.elections.ElectionScheduler;
import au.id.tindall.distalg.raft.exceptions.AlreadyRunningException;
import au.id.tindall.distalg.raft.exceptions.NotRunningException;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.rpc.client.ClientRequestMessage;
import au.id.tindall.distalg.raft.rpc.client.ClientResponseMessage;
import au.id.tindall.distalg.raft.rpc.clustermembership.ClusterMembershipRequest;
import au.id.tindall.distalg.raft.rpc.clustermembership.ClusterMembershipResponse;
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

    private static final int POLL_WARN_THRESHOLD_MS = 50;
    private static final Logger LOGGER = getLogger();

    private final PersistentState<I> persistentState;
    private final ServerStateFactory<I> serverStateFactory;
    private final StateMachine stateMachine;
    private final Cluster<I> cluster;
    private final ElectionScheduler electionScheduler;
    private ServerState<I> state;
    private ServerDriver serverDriver;

    public ServerImpl(PersistentState<I> persistentState, ServerStateFactory<I> serverStateFactory, StateMachine stateMachine, Cluster<I> cluster, ElectionScheduler electionScheduler) {
        this.persistentState = persistentState;
        this.serverStateFactory = serverStateFactory;
        this.stateMachine = stateMachine;
        this.cluster = cluster;
        this.electionScheduler = electionScheduler;
    }

    @Override
    public synchronized boolean poll() {
        long startTimeMillis = System.currentTimeMillis();
        long pollDurationMillis = 0;
        final Optional<RpcMessage<I>> message = cluster.poll();
        try {
            pollDurationMillis = System.currentTimeMillis() - startTimeMillis;
            message.ifPresent(this::handle);
            return message.isPresent();
        } finally {
            long totalDurationMillis = System.currentTimeMillis() - startTimeMillis;
            if (totalDurationMillis > POLL_WARN_THRESHOLD_MS) {
                LOGGER.warn("Message handling took {}ms, polling took {}ms, message was {}", totalDurationMillis, pollDurationMillis, message.orElse(null));
            }
        }
    }

    @Override
    public synchronized boolean timeoutNowIfDue() {
        if (electionScheduler.shouldTimeout()) {
            LOGGER.debug("Election timeout occurred: server {}", persistentState.getId());
            electionTimeout();
            return true;
        }
        return false;
    }

    @Override
    public synchronized void start() {
        start(NoOpServerDriver.INSTANCE);
    }

    @Override
    public synchronized void start(ServerDriver serverDriver) {
        if (state != null) {
            throw new AlreadyRunningException("Can't start, server is already started!");
        }
        if (this.serverDriver != null) {
            closeQuietly(this.serverDriver);
        }
        this.serverDriver = serverDriver;
        updateState(serverStateFactory.createInitialState());
        serverDriver.start(this);
        cluster.onStart();
    }

    @Override
    public synchronized void stop() {
        if (state == null) {
            throw new NotRunningException("Can't stop, server is not started");
        }
        serverDriver.stop();
        updateState(null);
        cluster.onStop();
    }

    @Override
    public synchronized CompletableFuture<? extends ClientResponseMessage> handle(ClientRequestMessage<I> clientRequestMessage) {
        assertThatNodeIsRunning();
        return state.handle(clientRequestMessage);
    }

    @Override
    public synchronized CompletableFuture<? extends ClusterMembershipResponse> handle(ClusterMembershipRequest clusterMembershipRequest) {
        assertThatNodeIsRunning();
        return state.handle(clusterMembershipRequest);
    }

    @Override
    public synchronized void handle(RpcMessage<I> message) {
        assertThatNodeIsRunning();
        Result<I> result;
        do {
            result = state.handle(message);
            updateState(result.getNextState());
        } while (!result.isFinished());
    }

    @Override
    public synchronized void initialize() {
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
    public synchronized void transferLeadership() {
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
        synchronized (this) {
            if (state != null) {
                stop();
            }
        }
        closeQuietly(serverStateFactory, serverDriver, persistentState);
    }
}
