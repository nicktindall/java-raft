package au.id.tindall.distalg.raft.rpc.server;

import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.serialisation.MessageIdentifier;
import au.id.tindall.distalg.raft.serialisation.StreamingInput;
import au.id.tindall.distalg.raft.serialisation.StreamingOutput;

public class TimeoutNowMessage<I> extends RpcMessage<I> {

    private static final MessageIdentifier MESSAGE_IDENTIFIER = MessageIdentifier.registerMessageIdentifier("TimeoutNowMessage", TimeoutNowMessage.class);

    private final boolean earlyElection;

    public TimeoutNowMessage(Term term, I source) {
        this(term, source, false);
    }

    public TimeoutNowMessage(Term term, I source, boolean earlyElection) {
        super(term, source);
        this.earlyElection = earlyElection;
    }

    public boolean isEarlyElection() {
        return earlyElection;
    }

    @SuppressWarnings("unused")
    public TimeoutNowMessage(StreamingInput streamingInput) {
        super(streamingInput);
        this.earlyElection = streamingInput.readBoolean();
    }

    @Override
    public MessageIdentifier getMessageIdentifier() {
        return MESSAGE_IDENTIFIER;
    }

    @Override
    public void writeTo(StreamingOutput streamingOutput) {
        super.writeTo(streamingOutput);
        streamingOutput.writeBoolean(earlyElection);
    }
}
