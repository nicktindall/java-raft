package au.id.tindall.distalg.raft.serverstates;

import au.id.tindall.distalg.raft.client.responses.PendingClientRequestResponse;
import au.id.tindall.distalg.raft.client.responses.PendingRegisterClientResponse;
import au.id.tindall.distalg.raft.client.responses.PendingResponseRegistry;
import au.id.tindall.distalg.raft.client.sessions.ClientSessionStore;
import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.entries.ClientRegistrationEntry;
import au.id.tindall.distalg.raft.log.entries.StateMachineCommandEntry;
import au.id.tindall.distalg.raft.replication.LogReplicator;
import au.id.tindall.distalg.raft.replication.LogReplicatorFactory;
import au.id.tindall.distalg.raft.rpc.client.ClientRequestRequest;
import au.id.tindall.distalg.raft.rpc.client.ClientRequestResponse;
import au.id.tindall.distalg.raft.rpc.client.ClientRequestStatus;
import au.id.tindall.distalg.raft.rpc.client.RegisterClientRequest;
import au.id.tindall.distalg.raft.rpc.client.RegisterClientResponse;
import au.id.tindall.distalg.raft.rpc.client.RegisterClientStatus;
import au.id.tindall.distalg.raft.rpc.server.AppendEntriesResponse;
import au.id.tindall.distalg.raft.rpc.server.TransferLeadershipMessage;
import au.id.tindall.distalg.raft.state.PersistentState;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static au.id.tindall.distalg.raft.rpc.client.ClientRequestStatus.SESSION_EXPIRED;
import static au.id.tindall.distalg.raft.serverstates.Result.complete;
import static au.id.tindall.distalg.raft.serverstates.ServerStateType.LEADER;
import static java.util.Collections.singletonList;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.apache.logging.log4j.LogManager.getLogger;

public class Leader<ID extends Serializable> extends ServerState<ID> {

    private static final Logger LOGGER = getLogger();
    private static final Duration MINIMUM_INTERVAL_BETWEEN_TIMEOUT_NOW_MESSAGES = Duration.ofMillis(100);

    private final Map<ID, LogReplicator<ID>> replicators;
    private final PendingResponseRegistry pendingResponseRegistry;
    private final ClientSessionStore clientSessionStore;
    private ID transferringLeadershipTo;
    private Instant lastTimeoutNowMessageSent;

    public Leader(PersistentState<ID> persistentState, Log log, Cluster<ID> cluster, PendingResponseRegistry pendingResponseRegistry,
                  LogReplicatorFactory<ID> logReplicatorFactory, ServerStateFactory<ID> serverStateFactory,
                  ClientSessionStore clientSessionStore) {
        super(persistentState, log, cluster, serverStateFactory, persistentState.getId());
        replicators = createReplicators(logReplicatorFactory);
        this.pendingResponseRegistry = pendingResponseRegistry;
        this.clientSessionStore = clientSessionStore;
        this.transferringLeadershipTo = null;
    }

    @Override
    protected CompletableFuture<RegisterClientResponse<ID>> handle(RegisterClientRequest<ID> registerClientRequest) {
        if (transferringLeadershipTo != null) {
            return completedFuture(new RegisterClientResponse<>(RegisterClientStatus.NOT_LEADER, null, null));
        }
        int logEntryIndex = log.getNextLogIndex();
        ClientRegistrationEntry registrationEntry = new ClientRegistrationEntry(persistentState.getCurrentTerm(), logEntryIndex);
        log.appendEntries(log.getLastLogIndex(), singletonList(registrationEntry));
        replicateToEveryone();
        return pendingResponseRegistry.registerOutstandingResponse(logEntryIndex, new PendingRegisterClientResponse<>());
    }

    @Override
    protected CompletableFuture<ClientRequestResponse<ID>> handle(ClientRequestRequest<ID> clientRequestRequest) {
        if (transferringLeadershipTo != null) {
            return completedFuture(new ClientRequestResponse<>(ClientRequestStatus.NOT_LEADER, null, null));
        }
        if (!clientSessionStore.hasSession(clientRequestRequest.getClientId())) {
            return completedFuture(new ClientRequestResponse<>(SESSION_EXPIRED, null, null));
        }
        int logEntryIndex = log.getNextLogIndex();
        StateMachineCommandEntry stateMachineCommandEntry = new StateMachineCommandEntry(persistentState.getCurrentTerm(), clientRequestRequest.getClientId(),
                clientRequestRequest.getSequenceNumber(), clientRequestRequest.getCommand());
        log.appendEntries(log.getLastLogIndex(), singletonList(stateMachineCommandEntry));
        replicateToEveryone();
        return pendingResponseRegistry.registerOutstandingResponse(logEntryIndex, new PendingClientRequestResponse<>());
    }

    @Override
    protected Result<ID> handle(AppendEntriesResponse<ID> appendEntriesResponse) {
        if (messageIsNotStale(appendEntriesResponse)) {
            handleCurrentAppendResponse(appendEntriesResponse);
            if (appendEntriesResponse.isSuccess()) {
                sendTimeoutNowRequestIfReadyToTransfer();
            }
        }
        return complete(this);
    }

    @Override
    protected Result<ID> handle(TransferLeadershipMessage<ID> transferLeadershipMessage) {
        transferringLeadershipTo = replicators.entrySet().stream()
                .min((replicator1, replicator2) -> replicator2.getValue().getMatchIndex() - replicator1.getValue().getMatchIndex())
                .orElseThrow(() -> new IllegalStateException("No followers to transfer to!"))
                .getKey();
        sendTimeoutNowRequestIfReadyToTransfer();
        return complete(this);
    }

    @Override
    public ServerStateType getServerStateType() {
        return LEADER;
    }

    private void sendTimeoutNowRequestIfReadyToTransfer() {
        if (transferringLeadershipTo != null
                && replicators.get(transferringLeadershipTo).getMatchIndex() == getLog().getLastLogIndex()
                && minimumIntervalBetweenTimeoutNowMessagesHasPassed()) {
            cluster.sendTimeoutNowRequest(persistentState.getCurrentTerm(), transferringLeadershipTo);
            lastTimeoutNowMessageSent = Instant.now();
        }
    }

    private boolean minimumIntervalBetweenTimeoutNowMessagesHasPassed() {
        return lastTimeoutNowMessageSent == null || lastTimeoutNowMessageSent.plus(MINIMUM_INTERVAL_BETWEEN_TIMEOUT_NOW_MESSAGES).isBefore(Instant.now());
    }

    private void handleCurrentAppendResponse(AppendEntriesResponse<ID> appendEntriesResponse) {
        ID remoteServerId = appendEntriesResponse.getSource();
        LogReplicator<ID> sourceReplicator = replicators.get(remoteServerId);
        if (appendEntriesResponse.isSuccess()) {
            int lastAppendedIndex = appendEntriesResponse.getAppendedIndex()
                    .orElseThrow(() -> new IllegalStateException("Append entries response was success with no appendedIndex"));
            sourceReplicator.logSuccessResponse(lastAppendedIndex);
            if (updateCommitIndex()) {
                replicateToEveryone();
            } else if (sourceReplicator.getNextIndex() <= log.getLastLogIndex()) {
                sourceReplicator.replicate();
            }
        } else {
            sourceReplicator.logFailedResponse();
            sourceReplicator.replicate();
        }
    }

    private boolean updateCommitIndex() {
        List<Integer> followerMatchIndices = replicators.values().stream()
                .map(LogReplicator::getMatchIndex)
                .collect(toList());
        return log.updateCommitIndex(followerMatchIndices, persistentState.getCurrentTerm()).isPresent();
    }

    private void replicateToEveryone() {
        replicators.values().forEach(LogReplicator::replicate);
    }

    private Map<ID, LogReplicator<ID>> createReplicators(LogReplicatorFactory<ID> logReplicatorFactory) {
        int defaultNextIndex = log.getNextLogIndex();
        return new HashMap<>(cluster.getOtherMemberIds().stream()
                .collect(toMap(identity(), id -> logReplicatorFactory.createLogReplicator(log, persistentState.getCurrentTerm(), cluster, id, defaultNextIndex))));
    }

    @Override
    public void enterState() {
        LOGGER.debug("Server entering Leader state");
        replicateToEveryone();
        replicators.values().forEach(LogReplicator::start);
    }

    @Override
    public void leaveState() {
        pendingResponseRegistry.dispose();
        replicators.values().forEach(LogReplicator::stop);
    }
}
