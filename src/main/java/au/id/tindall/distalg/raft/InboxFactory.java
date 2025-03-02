package au.id.tindall.distalg.raft;

import au.id.tindall.distalg.raft.comms.Inbox;

public interface InboxFactory<I> {

    Inbox<I> createInbox(I node);
}
