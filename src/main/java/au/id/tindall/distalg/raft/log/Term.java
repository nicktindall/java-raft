package au.id.tindall.distalg.raft.log;

import java.util.Objects;

public class Term {

    private final int number;

    public Term(int number) {
        this.number = number;
    }

    public boolean isLessThan(Term otherTerm) {
        return number < otherTerm.number;
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
