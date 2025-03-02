package au.id.tindall.distalg.raft.rpc.server;

import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.serialisation.MessageIdentifier;
import au.id.tindall.distalg.raft.serialisation.StreamingInput;
import au.id.tindall.distalg.raft.serialisation.StreamingOutput;

import java.util.Optional;

public class RequestVoteRequest<I> extends RpcMessage<I> {

    private static final MessageIdentifier MESSAGE_IDENTIFIER = MessageIdentifier.registerMessageIdentifier("RequestVoteRequest", RequestVoteRequest.class);

    private final I candidateId;
    private final int lastLogIndex;
    private final Term lastLogTerm;
    private final boolean earlyElection;

    public RequestVoteRequest(Term term, I candidateId, int lastLogIndex, Optional<Term> lastLogTerm, boolean earlyElection) {
        super(term, candidateId);
        this.candidateId = candidateId;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm.orElse(null);
        this.earlyElection = earlyElection;
    }

    @SuppressWarnings("unused")
    public RequestVoteRequest(StreamingInput streamingInput) {
        super(streamingInput);
        this.candidateId = streamingInput.readIdentifier();
        this.lastLogIndex = streamingInput.readInteger();
        this.lastLogTerm = streamingInput.readNullable(StreamingInput::readStreamable);
        this.earlyElection = streamingInput.readBoolean();
    }

    public I getCandidateId() {
        return candidateId;
    }

    public int getLastLogIndex() {
        return lastLogIndex;
    }

    public Optional<Term> getLastLogTerm() {
        return Optional.ofNullable(lastLogTerm);
    }

    public boolean isEarlyElection() {
        return earlyElection;
    }

    @Override
    public MessageIdentifier getMessageIdentifier() {
        return MESSAGE_IDENTIFIER;
    }

    @Override
    public void writeTo(StreamingOutput streamingOutput) {
        super.writeTo(streamingOutput);
        streamingOutput.writeIdentifier(candidateId);
        streamingOutput.writeInteger(lastLogIndex);
        streamingOutput.writeNullable(lastLogTerm, StreamingOutput::writeStreamable);
        streamingOutput.writeBoolean(earlyElection);
    }

    @Override
    public String toString() {
        return "RequestVoteRequest{" +
                "candidateId=" + candidateId +
                ", lastLogIndex=" + lastLogIndex +
                ", lastLogTerm=" + lastLogTerm +
                ", earlyElection=" + earlyElection +
                '}';
    }
}
