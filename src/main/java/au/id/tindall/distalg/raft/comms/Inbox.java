package au.id.tindall.distalg.raft.comms;

public interface Inbox<I> {

    boolean processNextMessage(MessageProcessor<I> messageProcessor);
}
