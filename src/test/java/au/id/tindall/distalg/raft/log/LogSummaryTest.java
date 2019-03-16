package au.id.tindall.distalg.raft.log;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Optional;

import org.junit.jupiter.api.Test;

public class LogSummaryTest {

    private static final Term LAST_LOG_TERM = new Term(3);
    private static final Term HIGHER_LAST_LOG_TERM = new Term(6);
    private static final int LAST_LOG_INDEX = 35;
    private static final int HIGHER_LAST_LOG_INDEX = 72;

    @Test
    public void compareTo_WillReturnZero_WhenBothLogsAreEmpty() {
        assertThat(new LogSummary(Optional.empty(), 0).compareTo(new LogSummary(Optional.empty(), 0))).isZero();
    }

    @Test
    public void compareTo_WillReturnNegative_WhenLogIsEmptyAndOtherIsNot() {
        assertThat(new LogSummary(Optional.empty(), 0).compareTo(new LogSummary(Optional.of(LAST_LOG_TERM), LAST_LOG_INDEX))).isNegative();
    }

    @Test
    public void compareTo_WillReturnPositive_WhenLogIsNotEmptyAndOtherIs() {
        assertThat(new LogSummary(Optional.of(LAST_LOG_TERM), LAST_LOG_INDEX).compareTo(new LogSummary(Optional.empty(), 0))).isPositive();
    }

    @Test
    public void compareTo_WillReturnZero_WhenLastLogTermAndLastLogIndexAreEqual() {
        assertThat(new LogSummary(Optional.of(LAST_LOG_TERM), LAST_LOG_INDEX)
                .compareTo(new LogSummary(Optional.of(LAST_LOG_TERM), LAST_LOG_INDEX))).isZero();
    }

    @Test
    public void compareTo_WillReturnNegative_WhenLastLogTermOfOtherIsHigher() {
        assertThat(new LogSummary(Optional.of(LAST_LOG_TERM), LAST_LOG_INDEX)
                .compareTo(new LogSummary(Optional.of(HIGHER_LAST_LOG_TERM), LAST_LOG_INDEX))).isNegative();
    }

    @Test
    public void compareTo_WillReturnPositive_WhenLastLogTermOfOtherIsLower() {
        assertThat(new LogSummary(Optional.of(HIGHER_LAST_LOG_TERM), LAST_LOG_INDEX)
                .compareTo(new LogSummary(Optional.of(LAST_LOG_TERM), LAST_LOG_INDEX))).isPositive();
    }

    @Test
    public void compareTo_WillReturnNegative_WhenLastLogTermsAreEqualAndLastLogIndexOfOtherIsHigher() {
        assertThat(new LogSummary(Optional.of(LAST_LOG_TERM), LAST_LOG_INDEX)
                .compareTo(new LogSummary(Optional.of(LAST_LOG_TERM), HIGHER_LAST_LOG_INDEX))).isNegative();
    }

    @Test
    public void compareTo_WillReturnPositive_WhenLastLogTermsAreEqualAndLastLogIndexOfOtherIsLower() {
        assertThat(new LogSummary(Optional.of(LAST_LOG_TERM), HIGHER_LAST_LOG_INDEX)
                .compareTo(new LogSummary(Optional.of(LAST_LOG_TERM), LAST_LOG_INDEX))).isPositive();
    }

    @Test
    public void equals_WillReturnTrue_WhenLastLogTermAndLastLogIndexAreEqual() {
        assertThat(new LogSummary(Optional.of(LAST_LOG_TERM), LAST_LOG_INDEX)).isEqualTo(new LogSummary(Optional.of(LAST_LOG_TERM), LAST_LOG_INDEX));
    }

    @Test
    public void equals_WillReturnFalse_WhenLastLogTermsDiffer() {
        assertThat(new LogSummary(Optional.of(HIGHER_LAST_LOG_TERM), LAST_LOG_INDEX)).isNotEqualTo(new LogSummary(Optional.of(LAST_LOG_TERM), LAST_LOG_INDEX));
    }

    @Test
    public void equals_WillReturnFalse_WhenLastLogIndicesDiffer() {
        assertThat(new LogSummary(Optional.of(LAST_LOG_TERM), HIGHER_LAST_LOG_INDEX)).isNotEqualTo(new LogSummary(Optional.of(LAST_LOG_TERM), LAST_LOG_INDEX));
    }
}