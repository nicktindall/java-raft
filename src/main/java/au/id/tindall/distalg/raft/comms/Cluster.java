package au.id.tindall.distalg.raft.comms;

import au.id.tindall.distalg.raft.cluster.Configuration;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.ConfigurationEntry;
import au.id.tindall.distalg.raft.log.entries.LogEntry;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;

public interface Cluster<I extends Serializable> {

    void onStart();

    void onStop();

    void sendAppendEntriesRequest(Term currentTerm, I destinationId, int prevLogIndex, Optional<Term> prevLogTerm, List<LogEntry> entriesToReplicate, int commitIndex);

    void sendAppendEntriesResponse(Term currentTerm, I destinationId, boolean success, Optional<Integer> appendedIndex);

    void sendRequestVoteRequest(Configuration<I> configuration, Term currentTerm, int lastLogIndex, Optional<Term> lastLogTerm, boolean earlyElection);

    void sendRequestVoteResponse(Term currentTerm, I destinationId, boolean granted);

    void sendTimeoutNowRequest(Term currentTerm, I destinationId);

    void sendInstallSnapshotResponse(Term currentTerm, I destinationId, boolean success, int lastIndex, int endOffset);

    void sendInstallSnapshotRequest(Term currentTerm, I destinationId, int lastIndex, Term lastTerm,
                                    ConfigurationEntry lastConfiguration, int snapshotOffset, int offset, byte[] data, boolean done);

}
