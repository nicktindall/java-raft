package au.id.tindall.distalg.raft.processors;

import au.id.tindall.distalg.raft.Server;
import au.id.tindall.distalg.raft.comms.Inbox;

import java.io.Serializable;

public class InboxProcessor<I extends Serializable> implements Processor<RaftProcessorGroup> {

    private final Server<I> server;
    private final Inbox<I> inbox;
    private final Runnable onStart;
    private final Runnable onStop;

    public InboxProcessor(Server<I> server, Inbox<I> inbox, Runnable onStart, Runnable onStop) {
        this.server = server;
        this.inbox = inbox;
        this.onStart = onStart;
        this.onStop = onStop;
    }

    @Override
    public void beforeFirst() {
        onStart.run();
    }

    @Override
    public void afterLast() {
        onStop.run();
    }

    @Override
    public ProcessResult process() {
        ProcessResult result = ProcessResult.IDLE;
        while (inbox.processNextMessage(server)) {
            result = ProcessResult.BUSY;
        }
        return result;
    }

    @Override
    public RaftProcessorGroup getGroup() {
        return RaftProcessorGroup.REPLICATION;
    }
}
