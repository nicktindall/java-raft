package au.id.tindall.distalg.raft.log;

import au.id.tindall.distalg.raft.serialisation.MessageIdentifier;
import au.id.tindall.distalg.raft.serialisation.Streamable;
import au.id.tindall.distalg.raft.serialisation.StreamingInput;
import au.id.tindall.distalg.raft.serialisation.StreamingOutput;

import java.util.Objects;

public class Term implements Comparable<Term>, Streamable {

    private static final MessageIdentifier MESSAGE_IDENTIFIER = MessageIdentifier.registerMessageIdentifier("Term", Term.class);
    public static final Term ZERO = new Term(0);

    private final int number;

    public Term(int number) {
        this.number = number;
    }

    public Term(StreamingInput streamingInput) {
        this(streamingInput.readInteger());
    }

    public boolean isLessThan(Term otherTerm) {
        return compareTo(otherTerm) < 0;
    }

    public boolean isGreaterThan(Term otherTerm) {
        return compareTo(otherTerm) > 0;
    }

    public Term next() {
        return new Term(number + 1);
    }

    public int getNumber() {
        return number;
    }

    @Override
    public int compareTo(Term otherTerm) {
        return number - otherTerm.number;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Term term = (Term) o;
        return number == term.number;
    }

    @Override
    public int hashCode() {
        return Objects.hash(number);
    }

    @Override
    public String toString() {
        return String.valueOf(number);
    }

    @Override
    public MessageIdentifier getMessageIdentifier() {
        return MESSAGE_IDENTIFIER;
    }

    @Override
    public void writeTo(StreamingOutput streamingOutput) {
        streamingOutput.writeInteger(number);
    }
}
