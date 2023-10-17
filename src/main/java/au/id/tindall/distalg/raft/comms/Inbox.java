package au.id.tindall.distalg.raft.comms;

import java.io.Serializable;

public interface Inbox<I extends Serializable> {

    boolean processNextMessage(MessageProcessor<I> messageProcessor);
}
