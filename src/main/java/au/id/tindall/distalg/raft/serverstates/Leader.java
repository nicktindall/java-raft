package au.id.tindall.distalg.raft.serverstates;

import au.id.tindall.distalg.raft.client.PendingRegisterClientResponse;
import au.id.tindall.distalg.raft.client.PendingResponseRegistry;
import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.ClientRegistrationEntry;
import au.id.tindall.distalg.raft.replication.LogReplicator;
import au.id.tindall.distalg.raft.replication.LogReplicatorFactory;
import au.id.tindall.distalg.raft.rpc.client.RegisterClientRequest;
import au.id.tindall.distalg.raft.rpc.client.RegisterClientResponse;
import au.id.tindall.distalg.raft.rpc.server.AppendEntriesResponse;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static au.id.tindall.distalg.raft.serverstates.Result.complete;
import static au.id.tindall.distalg.raft.serverstates.ServerStateType.LEADER;
import static java.util.Collections.singletonList;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class Leader<ID extends Serializable> extends ServerState<ID> {

    private final Map<ID, LogReplicator<ID>> replicators;
    private final PendingResponseRegistry pendingResponseRegistry;

    public Leader(Term currentTerm, Log log, Cluster<ID> cluster, PendingResponseRegistry pendingResponseRegistry,
                  LogReplicatorFactory<ID> logReplicatorFactory, ServerStateFactory<ID> serverStateFactory) {
        super(currentTerm, null, log, cluster, serverStateFactory);
        replicators = createReplicators(logReplicatorFactory);
        this.pendingResponseRegistry = pendingResponseRegistry;
    }

    @Override
    protected CompletableFuture<RegisterClientResponse<ID>> handle(RegisterClientRequest<ID> registerClientRequest) {
        int logEntryIndex = getLog().getNextLogIndex();
        ClientRegistrationEntry registrationEntry = new ClientRegistrationEntry(getCurrentTerm(), logEntryIndex);
        getLog().appendEntries(getLog().getLastLogIndex(), singletonList(registrationEntry));
        sendHeartbeatMessage();
        return pendingResponseRegistry.registerOutstandingResponse(logEntryIndex, new PendingRegisterClientResponse<>());
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
        if (appendEntriesResponse.isSuccess()) {
            int lastAppendedIndex = appendEntriesResponse.getAppendedIndex()
                    .orElseThrow(() -> new IllegalStateException("Append entries response was success with no appendedIndex"));
            replicators.get(remoteServerId).logSuccessResponse(lastAppendedIndex);
            if (updateCommitIndex()) {
                sendHeartbeatMessage();
            }
        } else {
            replicators.get(remoteServerId).logFailedResponse();
        }
    }

    private boolean updateCommitIndex() {
        List<Integer> followerMatchIndices = replicators.values().stream()
                .map(LogReplicator::getMatchIndex)
                .collect(toList());
        return getLog().updateCommitIndex(followerMatchIndices).isPresent();
    }

    public void sendHeartbeatMessage() {
        replicators.values()
                .forEach(replicator -> replicator.sendAppendEntriesRequest(getCurrentTerm(), getLog()));
    }

    private Map<ID, LogReplicator<ID>> createReplicators(LogReplicatorFactory<ID> logReplicatorFactory) {
        int defaultNextIndex = getLog().getNextLogIndex();
        return new HashMap<>(getCluster().getOtherMemberIds().stream()
                .collect(toMap(identity(), id -> logReplicatorFactory.createLogReplicator(getCluster(), id, defaultNextIndex))));
    }

    @Override
    public void dispose() {
        pendingResponseRegistry.dispose();
    }
}
