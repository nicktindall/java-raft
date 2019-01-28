package au.id.tindall.distalg.raft.log;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

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
}