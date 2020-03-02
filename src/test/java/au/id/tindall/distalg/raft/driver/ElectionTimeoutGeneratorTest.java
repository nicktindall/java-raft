package au.id.tindall.distalg.raft.driver;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ElectionTimeoutGeneratorTest {

    private static final int MINIMUM_ELECTION_TIMEOUT_IN_MILLISECONDS = 100;
    private static final int MAXIMUM_ELECTION_TIMEOUT_IN_MILLISECONDS = 150;

    @Mock
    private Random random;
    private ElectionTimeoutGenerator electionTimeoutGenerator;

    @BeforeEach
    void setUp() {
        electionTimeoutGenerator = new ElectionTimeoutGenerator(random, MINIMUM_ELECTION_TIMEOUT_IN_MILLISECONDS, MAXIMUM_ELECTION_TIMEOUT_IN_MILLISECONDS);
    }

    @Test
    void willReturnMinimumTimeoutWhenRandomIsZero() {
        when(random.nextInt(anyInt())).thenReturn(0);
        assertThat(electionTimeoutGenerator.next()).isEqualTo(MINIMUM_ELECTION_TIMEOUT_IN_MILLISECONDS);
    }

    @Test
    void willReturnMaximumTimeoutWhenRandomIsAtTopOfRange() {
        when(random.nextInt(anyInt())).thenReturn(49);
        assertThat(electionTimeoutGenerator.next()).isEqualTo(MAXIMUM_ELECTION_TIMEOUT_IN_MILLISECONDS - 1);
    }

    @Test
    void willThrowErrorWhenMinimumIsGreaterThanOrEqualToMaximum() {
        assertThatThrownBy(() -> {
            new ElectionTimeoutGenerator(random, MINIMUM_ELECTION_TIMEOUT_IN_MILLISECONDS, MINIMUM_ELECTION_TIMEOUT_IN_MILLISECONDS - 1);
        }).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void willThrowErrorWhenMinimumIsEqualToMaximum() {
        assertThatThrownBy(() -> {
            new ElectionTimeoutGenerator(random, MINIMUM_ELECTION_TIMEOUT_IN_MILLISECONDS, MINIMUM_ELECTION_TIMEOUT_IN_MILLISECONDS);
        }).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void willThrowErrorWhenMinimumElectionTimeoutIsNegative() {
        assertThatThrownBy(() -> {
            new ElectionTimeoutGenerator(random, -3, 5);
        }).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void willOnlyGenerateValuesInRange() {
        ElectionTimeoutGenerator electionTimeoutGenerator = new ElectionTimeoutGenerator(new Random(6101980L), MINIMUM_ELECTION_TIMEOUT_IN_MILLISECONDS, MAXIMUM_ELECTION_TIMEOUT_IN_MILLISECONDS);
        for (int i = 0; i < 1000; i++) {
            Long nextTimeout = electionTimeoutGenerator.next();
            assertThat(nextTimeout).isLessThan(MAXIMUM_ELECTION_TIMEOUT_IN_MILLISECONDS);
            assertThat(nextTimeout).isGreaterThanOrEqualTo(MINIMUM_ELECTION_TIMEOUT_IN_MILLISECONDS);
        }
    }
}