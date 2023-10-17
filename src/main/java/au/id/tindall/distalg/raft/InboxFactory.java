package au.id.tindall.distalg.raft;

import au.id.tindall.distalg.raft.comms.Inbox;

import java.io.Serializable;

public interface InboxFactory<I extends Serializable> {

    Inbox<I> createInbox(I node);
}
