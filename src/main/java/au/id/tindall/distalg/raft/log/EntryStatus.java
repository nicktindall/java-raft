package au.id.tindall.distalg.raft.log;

public enum EntryStatus {

    /**
     * There is an entry present in the log at that index
     */
    PRESENT,

    /**
     * The index is after the end of the log
     */
    AFTER_END,

    /**
     * The log entry has been truncated through log compaction
     */
    BEFORE_START
}
