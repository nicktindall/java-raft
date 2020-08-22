package au.id.tindall.distalg.raft.log;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class LogSummaryTest {

    private static final Term LAST_LOG_TERM = new Term(3);
    private static final Term HIGHER_LAST_LOG_TERM = new Term(6);
    private static final int LAST_LOG_INDEX = 35;
    private static final int HIGHER_LAST_LOG_INDEX = 72;

    @Test
    void compareTo_WillReturnZero_WhenBothLogsAreEmpty() {
        assertThat(new LogSummary(Optional.empty(), 0).compareTo(new LogSummary(Optional.empty(), 0))).isZero();
    }

    @Test
    void compareTo_WillReturnNegative_WhenLogIsEmptyAndOtherIsNot() {
        assertThat(new LogSummary(Optional.empty(), 0).compareTo(new LogSummary(Optional.of(LAST_LOG_TERM), LAST_LOG_INDEX))).isNegative();
    }

    @Test
    void compareTo_WillReturnPositive_WhenLogIsNotEmptyAndOtherIs() {
        assertThat(new LogSummary(Optional.of(LAST_LOG_TERM), LAST_LOG_INDEX).compareTo(new LogSummary(Optional.empty(), 0))).isPositive();
    }

    @Test
    void compareTo_WillReturnZero_WhenLastLogTermAndLastLogIndexAreEqual() {
        assertThat(new LogSummary(Optional.of(LAST_LOG_TERM), LAST_LOG_INDEX)
                .compareTo(new LogSummary(Optional.of(LAST_LOG_TERM), LAST_LOG_INDEX))).isZero();
    }

    @Test
    void compareTo_WillReturnNegative_WhenLastLogTermOfOtherIsHigher() {
        assertThat(new LogSummary(Optional.of(LAST_LOG_TERM), LAST_LOG_INDEX)
                .compareTo(new LogSummary(Optional.of(HIGHER_LAST_LOG_TERM), LAST_LOG_INDEX))).isNegative();
    }

    @Test
    void compareTo_WillReturnPositive_WhenLastLogTermOfOtherIsLower() {
        assertThat(new LogSummary(Optional.of(HIGHER_LAST_LOG_TERM), LAST_LOG_INDEX)
                .compareTo(new LogSummary(Optional.of(LAST_LOG_TERM), LAST_LOG_INDEX))).isPositive();
    }

    @Test
    void compareTo_WillReturnNegative_WhenLastLogTermsAreEqualAndLastLogIndexOfOtherIsHigher() {
        assertThat(new LogSummary(Optional.of(LAST_LOG_TERM), LAST_LOG_INDEX)
                .compareTo(new LogSummary(Optional.of(LAST_LOG_TERM), HIGHER_LAST_LOG_INDEX))).isNegative();
    }

    @Test
    void compareTo_WillReturnPositive_WhenLastLogTermsAreEqualAndLastLogIndexOfOtherIsLower() {
        assertThat(new LogSummary(Optional.of(LAST_LOG_TERM), HIGHER_LAST_LOG_INDEX)
                .compareTo(new LogSummary(Optional.of(LAST_LOG_TERM), LAST_LOG_INDEX))).isPositive();
    }

    @Test
    void equals_WillReturnTrue_WhenLastLogTermAndLastLogIndexAreEqual() {
        assertThat(new LogSummary(Optional.of(LAST_LOG_TERM), LAST_LOG_INDEX)).isEqualTo(new LogSummary(Optional.of(LAST_LOG_TERM), LAST_LOG_INDEX));
    }

    @Test
    void equals_WillReturnFalse_WhenLastLogTermsDiffer() {
        assertThat(new LogSummary(Optional.of(HIGHER_LAST_LOG_TERM), LAST_LOG_INDEX)).isNotEqualTo(new LogSummary(Optional.of(LAST_LOG_TERM), LAST_LOG_INDEX));
    }

    @Test
    void equals_WillReturnFalse_WhenLastLogIndicesDiffer() {
        assertThat(new LogSummary(Optional.of(LAST_LOG_TERM), HIGHER_LAST_LOG_INDEX)).isNotEqualTo(new LogSummary(Optional.of(LAST_LOG_TERM), LAST_LOG_INDEX));
    }
}