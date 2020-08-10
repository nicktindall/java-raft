package au.id.tindall.distalg.raft.serverstates;

import au.id.tindall.distalg.raft.client.responses.PendingClientRequestResponse;
import au.id.tindall.distalg.raft.client.responses.PendingRegisterClientResponse;
import au.id.tindall.distalg.raft.client.responses.PendingResponseRegistry;
import au.id.tindall.distalg.raft.client.sessions.ClientSessionStore;
import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.ClientRegistrationEntry;
import au.id.tindall.distalg.raft.log.entries.StateMachineCommandEntry;
import au.id.tindall.distalg.raft.replication.LogReplicator;
import au.id.tindall.distalg.raft.replication.LogReplicatorFactory;
import au.id.tindall.distalg.raft.rpc.client.ClientRequestRequest;
import au.id.tindall.distalg.raft.rpc.client.ClientRequestResponse;
import au.id.tindall.distalg.raft.rpc.client.RegisterClientRequest;
import au.id.tindall.distalg.raft.rpc.client.RegisterClientResponse;
import au.id.tindall.distalg.raft.rpc.server.AppendEntriesResponse;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static au.id.tindall.distalg.raft.rpc.client.ClientRequestStatus.SESSION_EXPIRED;
import static au.id.tindall.distalg.raft.serverstates.Result.complete;
import static au.id.tindall.distalg.raft.serverstates.ServerStateType.LEADER;
import static java.util.Collections.singletonList;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.apache.logging.log4j.LogManager.getLogger;

public class Leader<ID extends Serializable> extends ServerState<ID> {

    private static final Logger LOGGER = getLogger();

    private final Map<ID, LogReplicator<ID>> replicators;
    private final PendingResponseRegistry pendingResponseRegistry;
    private final ClientSessionStore clientSessionStore;

    public Leader(Term currentTerm, Log log, Cluster<ID> cluster, PendingResponseRegistry pendingResponseRegistry,
                  LogReplicatorFactory<ID> logReplicatorFactory, ServerStateFactory<ID> serverStateFactory,
                  ClientSessionStore clientSessionStore, ID id) {
        super(currentTerm, null, log, cluster, serverStateFactory, id);
        replicators = createReplicators(logReplicatorFactory);
        this.pendingResponseRegistry = pendingResponseRegistry;
        this.clientSessionStore = clientSessionStore;
    }

    @Override
    protected CompletableFuture<RegisterClientResponse<ID>> handle(RegisterClientRequest<ID> registerClientRequest) {
        int logEntryIndex = getLog().getNextLogIndex();
        ClientRegistrationEntry registrationEntry = new ClientRegistrationEntry(getCurrentTerm(), logEntryIndex);
        getLog().appendEntries(getLog().getLastLogIndex(), singletonList(registrationEntry));
        replicateToEveryone();
        return pendingResponseRegistry.registerOutstandingResponse(logEntryIndex, new PendingRegisterClientResponse<>());
    }

    @Override
    protected CompletableFuture<ClientRequestResponse<ID>> handle(ClientRequestRequest<ID> clientRequestRequest) {
        if (!clientSessionStore.hasSession(clientRequestRequest.getClientId())) {
            return CompletableFuture.completedFuture(new ClientRequestResponse<>(SESSION_EXPIRED, null, null));
        }
        int logEntryIndex = getLog().getNextLogIndex();
        StateMachineCommandEntry stateMachineCommandEntry = new StateMachineCommandEntry(getCurrentTerm(), clientRequestRequest.getClientId(),
                clientRequestRequest.getSequenceNumber(), clientRequestRequest.getCommand());
        getLog().appendEntries(getLog().getLastLogIndex(), singletonList(stateMachineCommandEntry));
        replicateToEveryone();
        return pendingResponseRegistry.registerOutstandingResponse(logEntryIndex, new PendingClientRequestResponse<>());
    }

    @Override
    protected Result<ID> handle(AppendEntriesResponse<ID> appendEntriesResponse) {
        if (messageIsNotStale(appendEntriesResponse)) {
            handleCurrentAppendResponse(appendEntriesResponse);
        }
        return complete(this);
    }

    @Override
    public ServerStateType getServerStateType() {
        return LEADER;
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
            } else if (sourceReplicator.getNextIndex() <= getLog().getLastLogIndex()) {
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
        return getLog().updateCommitIndex(followerMatchIndices, getCurrentTerm()).isPresent();
    }

    private void replicateToEveryone() {
        replicators.values().forEach(LogReplicator::replicate);
    }

    private Map<ID, LogReplicator<ID>> createReplicators(LogReplicatorFactory<ID> logReplicatorFactory) {
        int defaultNextIndex = getLog().getNextLogIndex();
        return new HashMap<>(getCluster().getOtherMemberIds().stream()
                .collect(toMap(identity(), id -> logReplicatorFactory.createLogReplicator(getLog(), getCurrentTerm(), getCluster(), id, defaultNextIndex))));
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
