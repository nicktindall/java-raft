package au.id.tindall.distalg.raft.comms;

import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.ConfigurationEntry;
import au.id.tindall.distalg.raft.log.entries.LogEntry;
import au.id.tindall.distalg.raft.rpc.server.RpcMessage;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public interface Cluster<I extends Serializable> {

    void onStart();

    void onStop();

    boolean isQuorum(Set<I> receivedVotes);

    Set<I> getOtherMemberIds();

    void sendAppendEntriesRequest(Term currentTerm, I destinationId, int prevLogIndex, Optional<Term> prevLogTerm, List<LogEntry> entriesToReplicate, int commitIndex);

    void sendAppendEntriesResponse(Term currentTerm, I destinationId, boolean success, Optional<Integer> appendedIndex);

    void sendRequestVoteRequest(Term currentTerm, int lastLogIndex, Optional<Term> lastLogTerm);

    void sendRequestVoteResponse(Term currentTerm, I destinationId, boolean granted);

    void sendTimeoutNowRequest(Term currentTerm, I destinationId);

    void sendInstallSnapshotResponse(Term currentTerm, I destinationId, boolean success, int lastIndex, int endOffset);

    void sendInstallSnapshotRequest(Term currentTerm, I destinationId, int lastIndex, Term lastTerm,
                                    ConfigurationEntry lastConfiguration, int snapshotOffset, int offset, byte[] data, boolean done);

    Optional<RpcMessage<I>> poll();
}
