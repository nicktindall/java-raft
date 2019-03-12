package au.id.tindall.distalg.raft;

import java.util.Arrays;

import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.entries.LogEntry;

public class DomainUtils {

    public static Log logContaining(LogEntry... entries) {
        Log log = new Log();
        log.appendEntries(0, Arrays.asList(entries));
        return log;
    }
}
