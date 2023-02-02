package au.id.tindall.distalg.raft.log;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TermTest {

    @Test
    void equals_WillReturnTrue_WhenNumberIsEqual() {
        assertThat(new Term(1)).isEqualTo(new Term(1));
    }

    @Test
    void equals_WillReturnFalse_WhenNumberIsNowEqual() {
        assertThat(new Term(1)).isNotEqualTo(new Term(2));
    }

    @Test
    void isLessThan_WillReturnTrue_WhenPassedTermIsGreaterThanCalledTerm() {
        assertThat(new Term(1).isLessThan(new Term(3))).isTrue();
    }

    @Test
    void isLessThan_WillReturnFalse_WhenPassedTermIsEqualToCalledTerm() {
        assertThat(new Term(1).isLessThan(new Term(1))).isFalse();
    }

    @Test
    void isLessThan_WillReturnFalse_WhenPassedTermIsLessThanCalledTerm() {
        assertThat(new Term(3).isLessThan(new Term(1))).isFalse();
    }

    @Test
    void isGreaterThan_WillReturnTrue_WhenPassedTermIsGreaterThanCalledTerm() {
        assertThat(new Term(3).isGreaterThan(new Term(1))).isTrue();
    }

    @Test
    void isGreaterThan_WillReturnFalse_WhenPassedTermIsEqualToCalledTerm() {
        assertThat(new Term(1).isGreaterThan(new Term(1))).isFalse();
    }

    @Test
    void isGreaterThan_WillReturnFalse_WhenPassedTermIsGreaterThanCalledTerm() {
        assertThat(new Term(1).isGreaterThan(new Term(3))).isFalse();
    }

    @Test
    void compareTo_WillReturnNegative_WhenOtherIsGreater() {
        assertThat(new Term(100)).isLessThan(new Term(200));
    }

    @Test
    void compareTo_WillReturnZero_WhenOtherIsEqual() {
        assertThat(new Term(100)).isEqualByComparingTo(new Term(100));
    }

    @Test
    void compareTo_WillReturnPositive_WhenOtherIsLess() {
        assertThat(new Term(100)).isGreaterThan(new Term(50));
    }

    @Test
    void next_WillProduceTheNextTerm() {
        assertThat(new Term(1).next()).isEqualTo(new Term(2));
    }
}