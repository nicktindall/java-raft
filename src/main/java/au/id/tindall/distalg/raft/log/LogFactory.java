package au.id.tindall.distalg.raft.log;

import au.id.tindall.distalg.raft.log.storage.LogStorage;

public class LogFactory {

    public Log createLog(LogStorage logStorage) {
        return new Log(logStorage);
    }
}
