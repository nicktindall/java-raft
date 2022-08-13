package au.id.tindall.distalg.raft.serverstates;

import au.id.tindall.distalg.raft.client.responses.PendingClientRequestResponse;
import au.id.tindall.distalg.raft.client.responses.PendingRegisterClientResponse;
import au.id.tindall.distalg.raft.client.responses.PendingResponseRegistry;
import au.id.tindall.distalg.raft.client.sessions.ClientSessionStore;
import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.entries.ClientRegistrationEntry;
import au.id.tindall.distalg.raft.log.entries.StateMachineCommandEntry;
import au.id.tindall.distalg.raft.replication.ReplicationManager;
import au.id.tindall.distalg.raft.rpc.client.ClientRequestRequest;
import au.id.tindall.distalg.raft.rpc.client.ClientRequestResponse;
import au.id.tindall.distalg.raft.rpc.client.ClientRequestStatus;
import au.id.tindall.distalg.raft.rpc.client.RegisterClientRequest;
import au.id.tindall.distalg.raft.rpc.client.RegisterClientResponse;
import au.id.tindall.distalg.raft.rpc.client.RegisterClientStatus;
import au.id.tindall.distalg.raft.rpc.clustermembership.AddServerRequest;
import au.id.tindall.distalg.raft.rpc.clustermembership.AddServerResponse;
import au.id.tindall.distalg.raft.rpc.clustermembership.RemoveServerRequest;
import au.id.tindall.distalg.raft.rpc.clustermembership.RemoveServerResponse;
import au.id.tindall.distalg.raft.rpc.server.AppendEntriesResponse;
import au.id.tindall.distalg.raft.rpc.server.TransferLeadershipMessage;
import au.id.tindall.distalg.raft.serverstates.clustermembership.ClusterMembershipChangeManager;
import au.id.tindall.distalg.raft.serverstates.leadershiptransfer.LeadershipTransfer;
import au.id.tindall.distalg.raft.state.PersistentState;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;

import static au.id.tindall.distalg.raft.rpc.client.ClientRequestStatus.SESSION_EXPIRED;
import static au.id.tindall.distalg.raft.serverstates.Result.complete;
import static au.id.tindall.distalg.raft.serverstates.ServerStateType.LEADER;
import static au.id.tindall.distalg.raft.util.Closeables.closeQuietly;
import static java.util.Collections.singletonList;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.logging.log4j.LogManager.getLogger;

public class Leader<ID extends Serializable> extends ServerState<ID> {

    private static final Logger LOGGER = getLogger();

    private final PendingResponseRegistry pendingResponseRegistry;
    private final ReplicationManager<ID> replicationManager;
    private final ClientSessionStore clientSessionStore;
    private final LeadershipTransfer<ID> leadershipTransfer;
    private final ClusterMembershipChangeManager<ID> clusterMembershipChangeManager;

    public Leader(PersistentState<ID> persistentState, Log log, Cluster<ID> cluster, PendingResponseRegistry pendingResponseRegistry,
                  ServerStateFactory<ID> serverStateFactory, ReplicationManager<ID> replicationManager, ClientSessionStore clientSessionStore,
                  LeadershipTransfer<ID> leadershipTransfer, ClusterMembershipChangeManager<ID> clusterMembershipChangeManager) {
        super(persistentState, log, cluster, serverStateFactory, persistentState.getId());
        this.pendingResponseRegistry = pendingResponseRegistry;
        this.replicationManager = replicationManager;
        this.clientSessionStore = clientSessionStore;
        this.leadershipTransfer = leadershipTransfer;
        this.clusterMembershipChangeManager = clusterMembershipChangeManager;
    }

    @Override
    protected CompletableFuture<RegisterClientResponse<ID>> handle(RegisterClientRequest<ID> registerClientRequest) {
        if (leadershipTransfer.isInProgress()) {
            return completedFuture(new RegisterClientResponse<>(RegisterClientStatus.NOT_LEADER, null, null));
        }
        int logEntryIndex = log.getNextLogIndex();
        ClientRegistrationEntry registrationEntry = new ClientRegistrationEntry(persistentState.getCurrentTerm(), logEntryIndex);
        log.appendEntries(log.getLastLogIndex(), singletonList(registrationEntry));
        replicationManager.replicate();
        return pendingResponseRegistry.registerOutstandingResponse(logEntryIndex, new PendingRegisterClientResponse<>());
    }

    @Override
    protected CompletableFuture<ClientRequestResponse<ID>> handle(ClientRequestRequest<ID> clientRequestRequest) {
        if (leadershipTransfer.isInProgress()) {
            return completedFuture(new ClientRequestResponse<>(ClientRequestStatus.NOT_LEADER, null, null));
        }
        if (!clientSessionStore.hasSession(clientRequestRequest.getClientId())) {
            return completedFuture(new ClientRequestResponse<>(SESSION_EXPIRED, null, null));
        }
        int logEntryIndex = log.getNextLogIndex();
        StateMachineCommandEntry stateMachineCommandEntry = new StateMachineCommandEntry(persistentState.getCurrentTerm(), clientRequestRequest.getClientId(),
                clientRequestRequest.getLastResponseReceived(), clientRequestRequest.getSequenceNumber(), clientRequestRequest.getCommand());
        log.appendEntries(log.getLastLogIndex(), singletonList(stateMachineCommandEntry));
        replicationManager.replicate();
        return pendingResponseRegistry.registerOutstandingResponse(logEntryIndex, new PendingClientRequestResponse<>());
    }

    @Override
    protected Result<ID> handle(AppendEntriesResponse<ID> appendEntriesResponse) {
        if (messageIsNotStale(appendEntriesResponse)) {
            handleCurrentAppendResponse(appendEntriesResponse);
            if (appendEntriesResponse.isSuccess() && leadershipTransfer.isInProgress()) {
                leadershipTransfer.sendTimeoutNowRequestIfReadyToTransfer();
            }
        }
        return complete(this);
    }

    @Override
    protected Result<ID> handle(TransferLeadershipMessage<ID> transferLeadershipMessage) {
        leadershipTransfer.start();
        return complete(this);
    }

    @Override
    public ServerStateType getServerStateType() {
        return LEADER;
    }

    @Override
    protected CompletableFuture<AddServerResponse> handle(AddServerRequest<ID> addServerRequest) {
        return clusterMembershipChangeManager.addServer(addServerRequest.getNewServer());
    }

    @Override
    protected CompletableFuture<RemoveServerResponse> handle(RemoveServerRequest<ID> removeServerRequest) {
        if (persistentState.getId().equals(removeServerRequest.getOldServer())) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException("Can't remove current leader"));
        }
        return clusterMembershipChangeManager.removeServer(removeServerRequest.getOldServer());
    }

    private void handleCurrentAppendResponse(AppendEntriesResponse<ID> appendEntriesResponse) {
        ID remoteServerId = appendEntriesResponse.getSource();
        if (appendEntriesResponse.isSuccess()) {
            int lastAppendedIndex = appendEntriesResponse.getAppendedIndex()
                    .orElseThrow(() -> new IllegalStateException("Append entries response was success with no appendedIndex"));
            replicationManager.logSuccessResponse(remoteServerId, lastAppendedIndex);
            clusterMembershipChangeManager.logSuccessResponse(remoteServerId, lastAppendedIndex);
            if (updateCommitIndex()) {
                replicationManager.replicate();
            } else {
                replicationManager.replicateIfTrailingIndex(remoteServerId, log.getLastLogIndex());
            }
        } else {
            clusterMembershipChangeManager.logFailureResponse(remoteServerId);
            replicationManager.logFailedResponse(remoteServerId, appendEntriesResponse.getAppendedIndex().orElse(null));
            replicationManager.replicate(remoteServerId);
        }
    }

    private boolean updateCommitIndex() {
        return log.updateCommitIndex(replicationManager.getFollowerMatchIndices(), persistentState.getCurrentTerm()).isPresent();
    }

    @Override
    public void enterState() {
        LOGGER.debug("Server entering Leader state");
        replicationManager.start();
        replicationManager.replicate();
        log.addEntryCommittedEventHandler(clusterMembershipChangeManager);
    }

    @Override
    public void leaveState() {
        log.removeEntryCommittedEventHandler(clusterMembershipChangeManager);
        closeQuietly(clusterMembershipChangeManager);
        pendingResponseRegistry.dispose();
        replicationManager.stop();
    }
}
