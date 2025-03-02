package au.id.tindall.distalg.raft.rpc.server;

import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.LogEntry;
import au.id.tindall.distalg.raft.serialisation.MessageIdentifier;
import au.id.tindall.distalg.raft.serialisation.StreamingInput;
import au.id.tindall.distalg.raft.serialisation.StreamingOutput;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static java.util.List.copyOf;

public class AppendEntriesRequest<I> extends RpcMessage<I> {

    private static final MessageIdentifier MESSAGE_IDENTIFIER = MessageIdentifier.registerMessageIdentifier("AppendEntriesRequest", AppendEntriesRequest.class);

    private final I leaderId;
    private final int prevLogIndex;
    private final Term prevLogTerm;
    private final List<LogEntry> entries;
    private final int leaderCommit;

    public AppendEntriesRequest(Term term, I leaderId, int prevLogIndex, Optional<Term> prevLogTerm, List<LogEntry> entries, int leaderCommit) {
        super(term, leaderId);
        this.leaderId = leaderId;
        this.prevLogIndex = prevLogIndex;
        this.prevLogTerm = prevLogTerm.orElse(null);
        this.entries = copyOf(entries);
        this.leaderCommit = leaderCommit;
    }

    @SuppressWarnings("unused")
    public AppendEntriesRequest(StreamingInput streamingInput) {
        super(streamingInput);
        this.leaderId = streamingInput.readIdentifier();
        this.prevLogIndex = streamingInput.readInteger();
        this.prevLogTerm = streamingInput.readNullable(StreamingInput::readStreamable);
        this.entries = streamingInput.readList(ArrayList::new, StreamingInput::readStreamable);
        this.leaderCommit = streamingInput.readInteger();
    }

    public I getLeaderId() {
        return leaderId;
    }

    public int getPrevLogIndex() {
        return prevLogIndex;
    }

    public Term getPrevLogTerm() {
        return prevLogTerm;
    }

    public List<LogEntry> getEntries() {
        return entries;
    }

    public int getLeaderCommit() {
        return leaderCommit;
    }

    @Override
    public MessageIdentifier getMessageIdentifier() {
        return MESSAGE_IDENTIFIER;
    }

    @Override
    public void writeTo(StreamingOutput streamingOutput) {
        super.writeTo(streamingOutput);
        streamingOutput.writeIdentifier(leaderId);
        streamingOutput.writeInteger(prevLogIndex);
        streamingOutput.writeNullable(prevLogTerm, StreamingOutput::writeStreamable);
        streamingOutput.writeList(entries, StreamingOutput::writeStreamable);
        streamingOutput.writeInteger(leaderCommit);
    }

    @Override
    public String toString() {
        return "AppendEntriesRequest{" +
                "leaderId=" + leaderId +
                ", prevLogIndex=" + prevLogIndex +
                ", prevLogTerm=" + prevLogTerm +
                ", entries.size()=" + entries.size() +
                ", leaderCommit=" + leaderCommit +
                "} " + super.toString();
    }
}
