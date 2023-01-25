package au.id.tindall.distalg.raft;

import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.ConfigurationEntry;
import au.id.tindall.distalg.raft.log.entries.LogEntry;
import au.id.tindall.distalg.raft.state.InMemorySnapshot;

import java.util.Arrays;
import java.util.Set;

public class DomainUtils {

    public static Log logContaining(LogEntry... entries) {
        Log log = new Log();
        log.appendEntries(0, Arrays.asList(entries));
        return log;
    }

    public static InMemorySnapshot createInMemorySnapshot(int lastIndex, Term lastTerm) {
        final InMemorySnapshot nextSnapshot = new InMemorySnapshot(lastIndex, lastTerm, new ConfigurationEntry(lastTerm, Set.of(1L, 2L)));
        nextSnapshot.writeBytes(0, "sessions".getBytes());
        nextSnapshot.finaliseSessions();
        nextSnapshot.writeBytes(nextSnapshot.getSnapshotOffset(), "snapshot".getBytes());
        nextSnapshot.finalise();
        return nextSnapshot;
    }
}
