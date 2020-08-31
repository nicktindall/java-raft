package au.id.tindall.distalg.raft.log;

import java.io.Serializable;
import java.util.Objects;

public class Term implements Serializable, Comparable<Term> {

    private final int number;

    public Term(int number) {
        this.number = number;
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
        return "Term{" +
                "number=" + number +
                '}';
    }
}
