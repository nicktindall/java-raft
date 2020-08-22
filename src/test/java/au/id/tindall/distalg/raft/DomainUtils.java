package au.id.tindall.distalg.raft;

import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.entries.LogEntry;

import java.util.Arrays;

public class DomainUtils {

    public static Log logContaining(LogEntry... entries) {
        Log log = new Log();
        log.appendEntries(0, Arrays.asList(entries));
        return log;
    }
}
