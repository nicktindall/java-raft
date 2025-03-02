package au.id.tindall.distalg.raft.rpc.server;

import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.serialisation.MessageIdentifier;
import au.id.tindall.distalg.raft.serialisation.StreamingInput;
import au.id.tindall.distalg.raft.serialisation.StreamingOutput;

public class RequestVoteResponse<I> extends RpcMessage<I> {

    private static final MessageIdentifier MESSAGE_IDENTIFIER = MessageIdentifier.registerMessageIdentifier("RequestVoteResponse", RequestVoteResponse.class);

    private final boolean voteGranted;

    public RequestVoteResponse(Term term, I source, boolean voteGranted) {
        super(term, source);
        this.voteGranted = voteGranted;
    }

    @SuppressWarnings("unused")
    public RequestVoteResponse(StreamingInput streamingInput) {
        super(streamingInput);
        this.voteGranted = streamingInput.readBoolean();
    }

    public boolean isVoteGranted() {
        return voteGranted;
    }

    @Override
    public MessageIdentifier getMessageIdentifier() {
        return MESSAGE_IDENTIFIER;
    }

    @Override
    public void writeTo(StreamingOutput streamingOutput) {
        super.writeTo(streamingOutput);
        streamingOutput.writeBoolean(voteGranted);
    }

    @Override
    public String toString() {
        return "RequestVoteResponse{" +
                "voteGranted=" + voteGranted +
                "} " + super.toString();
    }
}
