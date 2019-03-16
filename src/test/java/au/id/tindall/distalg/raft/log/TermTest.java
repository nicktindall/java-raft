package au.id.tindall.distalg.raft.log;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

public class TermTest {

    @Test
    public void equals_WillReturnTrue_WhenNumberIsEqual() {
        assertThat(new Term(1)).isEqualTo(new Term(1));
    }

    @Test
    public void equals_WillReturnFalse_WhenNumberIsNowEqual() {
        assertThat(new Term(1)).isNotEqualTo(new Term(2));
    }

    @Test
    public void isLessThan_WillReturnTrue_WhenPassedTermIsGreaterThanCalledTerm() {
        assertThat(new Term(1).isLessThan(new Term(3))).isTrue();
    }

    @Test
    public void isLessThan_WillReturnFalse_WhenPassedTermIsEqualToCalledTerm() {
        assertThat(new Term(1).isLessThan(new Term(1))).isFalse();
    }

    @Test
    public void isLessThan_WillReturnFalse_WhenPassedTermIsLessThanCalledTerm() {
        assertThat(new Term(3).isLessThan(new Term(1))).isFalse();
    }

    @Test
    public void isGreaterThan_WillReturnTrue_WhenPassedTermIsGreaterThanCalledTerm() {
        assertThat(new Term(3).isGreaterThan(new Term(1))).isTrue();
    }

    @Test
    public void isGreaterThan_WillReturnFalse_WhenPassedTermIsEqualToCalledTerm() {
        assertThat(new Term(1).isGreaterThan(new Term(1))).isFalse();
    }

    @Test
    public void isGreaterThan_WillReturnFalse_WhenPassedTermIsGreaterThanCalledTerm() {
        assertThat(new Term(1).isGreaterThan(new Term(3))).isFalse();
    }

    @Test
    public void compareTo_WillReturnNegative_WhenOtherIsGreater() {
        assertThat(new Term(100).compareTo(new Term(200))).isNegative();
    }

    @Test
    public void compareTo_WillReturnZero_WhenOtherIsEqual() {
        assertThat(new Term(100).compareTo(new Term(100))).isZero();
    }

    @Test
    public void compareTo_WillReturnPositive_WhenOtherIsLess() {
        assertThat(new Term(100).compareTo(new Term(50))).isPositive();
    }

    @Test
    public void next_WillProduceTheNextTerm() {
        assertThat(new Term(1).next()).isEqualTo(new Term(2));
    }
}