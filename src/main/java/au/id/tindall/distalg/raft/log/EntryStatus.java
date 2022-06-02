package au.id.tindall.distalg.raft.log;

public enum EntryStatus {

    /**
     * There is an entry present in the log at that index
     */
    Present,

    /**
     * The index is after the end of the log
     */
    AfterEnd,

    /**
     * The log entry has been truncated through log compaction
     */
    BeforeStart
}
