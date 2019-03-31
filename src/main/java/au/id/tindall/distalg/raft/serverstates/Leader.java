package au.id.tindall.distalg.raft.serverstates;

import static au.id.tindall.distalg.raft.serverstates.Result.complete;
import static au.id.tindall.distalg.raft.serverstates.ServerStateType.LEADER;
import static java.util.Collections.singletonList;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import au.id.tindall.distalg.raft.client.ClientRegistry;
import au.id.tindall.distalg.raft.client.ClientRegistryFactory;
import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.ClientRegistrationEntry;
import au.id.tindall.distalg.raft.replication.LogReplicator;
import au.id.tindall.distalg.raft.replication.LogReplicatorFactory;
import au.id.tindall.distalg.raft.rpc.server.AppendEntriesResponse;
import au.id.tindall.distalg.raft.rpc.client.RegisterClientRequest;
import au.id.tindall.distalg.raft.rpc.client.RegisterClientResponse;

public class Leader<ID extends Serializable> extends ServerState<ID> {

    private final Map<ID, LogReplicator<ID>> replicators;
    private final ClientRegistry<ID> clientRegistry;

    public Leader(ID id, Term currentTerm, Log log, Cluster<ID> cluster, ClientRegistryFactory<ID> clientRegistryFactory,
                  LogReplicatorFactory<ID> logReplicatorFactory) {
        super(id, currentTerm, null, log, cluster);
        replicators = createReplicators(logReplicatorFactory);
        clientRegistry = clientRegistryFactory.createClientRegistry();
        clientRegistry.startListeningForCommitEvents(getLog());
    }

    @Override
    protected CompletableFuture<RegisterClientResponse<ID>> handle(RegisterClientRequest<ID> registerClientRequest) {
        int clientId = getLog().getNextLogIndex();
        ClientRegistrationEntry registrationEntry = new ClientRegistrationEntry(getCurrentTerm(), clientId);
        getLog().appendEntries(getLog().getLastLogIndex(), singletonList(registrationEntry));
        sendHeartbeatMessage();
        return clientRegistry.createResponseFuture(clientId);
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
            updateCommitIndex();
        } else {
            replicators.get(remoteServerId).logFailedResponse();
        }
    }

    private void updateCommitIndex() {
        List<Integer> followerMatchIndices = replicators.values().stream()
                .map(LogReplicator::getMatchIndex)
                .collect(toList());
        getLog().updateCommitIndex(followerMatchIndices);
    }

    public void sendHeartbeatMessage() {
        replicators.values()
                .forEach(replicator -> replicator.sendAppendEntriesRequest(getCurrentTerm(), getLog()));
    }

    private Map<ID, LogReplicator<ID>> createReplicators(LogReplicatorFactory<ID> logReplicatorFactory) {
        int defaultNextIndex = getLog().getNextLogIndex();
        return new HashMap<>(getCluster().getMemberIds().stream()
                .filter(memberId -> !getId().equals(memberId))
                .collect(toMap(identity(), id -> logReplicatorFactory.createLogReplicator(getId(), getCluster(), id, defaultNextIndex))));
    }

    @Override
    public void dispose() {
        clientRegistry.stopListeningForCommitEvents(getLog());
        clientRegistry.failAnyOutstandingRegistrations();
    }
}
