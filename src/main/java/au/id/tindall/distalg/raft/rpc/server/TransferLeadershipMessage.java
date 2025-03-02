package au.id.tindall.distalg.raft.rpc.server;

import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.serialisation.MessageIdentifier;

public class TransferLeadershipMessage<I> extends RpcMessage<I> {

    private static final MessageIdentifier MESSAGE_IDENTIFIER = MessageIdentifier.registerMessageIdentifier("TransferLeadershipMessage", TransferLeadershipMessage.class);

    public TransferLeadershipMessage(Term term, I serverId) {
        super(term, serverId);
    }

    @Override
    public MessageIdentifier getMessageIdentifier() {
        return MESSAGE_IDENTIFIER;
    }
}
