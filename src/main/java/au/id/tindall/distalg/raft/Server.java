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

public class Server<ID extends Serializable> implements Closeable {

    private static final int POLL_WARN_THRESHOLD_MS = 50;
    private static final Logger LOGGER = getLogger();

    private final PersistentState<ID> persistentState;
    private final ServerStateFactory<ID> serverStateFactory;
    private final StateMachine stateMachine;
    private final Cluster<ID> cluster;
    private final ElectionScheduler electionScheduler;
    private ServerState<ID> state;
    private ServerDriver serverDriver;

    public Server(PersistentState<ID> persistentState, ServerStateFactory<ID> serverStateFactory, StateMachine stateMachine, Cluster<ID> cluster, ElectionScheduler electionScheduler) {
        this.persistentState = persistentState;
        this.serverStateFactory = serverStateFactory;
        this.stateMachine = stateMachine;
        this.cluster = cluster;
        this.electionScheduler = electionScheduler;
    }

    public synchronized boolean poll() {
        long startTimeMillis = System.currentTimeMillis();
        long pollDurationMillis = 0;
        final Optional<RpcMessage<ID>> message = cluster.poll();
        try {
            pollDurationMillis = System.currentTimeMillis() - startTimeMillis;
            message.ifPresent(this::handle);
            return message.isPresent();
        } finally {
            long totalDurationMillis = System.currentTimeMillis() - startTimeMillis;
            if (totalDurationMillis > POLL_WARN_THRESHOLD_MS) {
                LOGGER.warn("Message handling took {}ms, polling took {}ms, message was {}", totalDurationMillis, pollDurationMillis, message.get());
            }
        }
    }

    public synchronized boolean timeoutNowIfDue() {
        if (electionScheduler.shouldTimeout()) {
            LOGGER.debug("Election timeout occurred: server {}", persistentState.getId());
            electionTimeout();
            return true;
        }
        return false;
    }

    public synchronized void start() {
        start(NoOpServerDriver.INSTANCE);
    }

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

    public synchronized void stop() {
        if (state == null) {
            throw new NotRunningException("Can't stop, server is not started");
        }
        serverDriver.stop();
        updateState(null);
        cluster.onStop();
    }

    public synchronized CompletableFuture<? extends ClientResponseMessage> handle(ClientRequestMessage<ID> clientRequestMessage) {
        assertThatNodeIsRunning();
        return state.handle(clientRequestMessage);
    }

    public synchronized CompletableFuture<? extends ClusterMembershipResponse> handle(ClusterMembershipRequest clusterMembershipRequest) {
        assertThatNodeIsRunning();
        return state.handle(clusterMembershipRequest);
    }

    public synchronized void handle(RpcMessage<ID> message) {
        assertThatNodeIsRunning();
        Result<ID> result;
        do {
            result = state.handle(message);
            updateState(result.getNextState());
        } while (!result.isFinished());
    }

    public synchronized void initialize() {
        persistentState.initialize();
    }

    private void assertThatNodeIsRunning() {
        if (state == null) {
            throw new NotRunningException("Server is not running, call start() before attempting to interact with it");
        }
    }

    private void updateState(ServerState<ID> nextState) {
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

    public synchronized void transferLeadership() {
        handle(new TransferLeadershipMessage<>(persistentState.getCurrentTerm(), persistentState.getId()));
    }

    public ID getId() {
        return persistentState.getId();
    }

    public Optional<ServerStateType> getState() {
        return Optional.ofNullable(state)
                .map(ServerState::getServerStateType);
    }

    public Log getLog() {
        return state.getLog();
    }

    public ClientSessionStore getClientSessionStore() {
        return serverStateFactory.getClientSessionStore();
    }

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
        closeQuietly(serverStateFactory, serverDriver);
    }
}
