package au.id.tindall.distalg.raft.log.entries;

import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.serialisation.MessageIdentifier;
import au.id.tindall.distalg.raft.serialisation.StreamingInput;
import au.id.tindall.distalg.raft.serialisation.StreamingOutput;

import java.util.HashSet;
import java.util.Set;

public class ConfigurationEntry extends LogEntry {

    private static final MessageIdentifier MESSAGE_IDENTIFIER = MessageIdentifier.registerMessageIdentifier("LogCE", ConfigurationEntry.class);

    private final Set<?> clusterMembers;

    public ConfigurationEntry(Term term, Set<?> clusterMembers) {
        super(term);
        this.clusterMembers = clusterMembers;
    }

    public ConfigurationEntry(StreamingInput streamingInput) {
        super(streamingInput);
        this.clusterMembers = streamingInput.readSet(HashSet::newHashSet, StreamingInput::readIdentifier);
    }

    public Set<?> getClusterMembers() {
        return clusterMembers;
    }

    @Override
    public MessageIdentifier getMessageIdentifier() {
        return MESSAGE_IDENTIFIER;
    }

    @Override
    public void writeTo(StreamingOutput streamingOutput) {
        super.writeTo(streamingOutput);
        streamingOutput.writeSet(clusterMembers, StreamingOutput::writeIdentifier);
    }
}
