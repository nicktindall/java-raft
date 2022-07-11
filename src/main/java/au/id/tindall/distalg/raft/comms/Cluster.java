package au.id.tindall.distalg.raft.comms;

import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.ConfigurationEntry;
import au.id.tindall.distalg.raft.log.entries.LogEntry;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public interface Cluster<ID extends Serializable> {

    boolean isQuorum(Set<ID> receivedVotes);

    Set<ID> getOtherMemberIds();

    void sendAppendEntriesRequest(Term currentTerm, ID destinationId, int prevLogIndex, Optional<Term> prevLogTerm, List<LogEntry> entriesToReplicate, int commitIndex);

    void sendAppendEntriesResponse(Term currentTerm, ID destinationId, boolean success, Optional<Integer> appendedIndex);

    void sendRequestVoteRequest(Term currentTerm, int lastLogIndex, Optional<Term> lastLogTerm);

    void sendRequestVoteResponse(Term currentTerm, ID destinationId, boolean granted);

    void sendTimeoutNowRequest(Term currentTerm, ID destinationId);

    void sendInstallSnapshotResponse(Term currentTerm, ID destinationId, boolean success, int lastIndex, int endOffset);

    void sendInstallSnapshotRequest(Term currentTerm, ID destinationId, int lastIndex, Term lastTerm,
                                    ConfigurationEntry lastConfiguration, int offset, byte[] data, boolean done);
}
