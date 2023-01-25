package au.id.tindall.distalg.raft.log.storage;

import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.ClientRegistrationEntry;
import au.id.tindall.distalg.raft.log.entries.ConfigurationEntry;
import au.id.tindall.distalg.raft.state.InMemorySnapshot;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Set;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;


@ExtendWith(MockitoExtension.class)
class BufferedTruncationCalculatorTest {

    private static final int TRUNCATION_BUFFER = 10;
    private static final Term TERM_ONE = new Term(1);
    private static final Term TERM_TWO = new Term(2);
    private static final Term TERM_THREE = new Term(3);
    private static final ConfigurationEntry LAST_CONFIG = new ConfigurationEntry(TERM_ONE, Set.of(1, 2, 3));

    private InMemoryLogStorage logStorage;

    @BeforeEach
    void setUp() {
        logStorage = new InMemoryLogStorage(TRUNCATION_BUFFER);
        IntStream.range(1, 101).forEach(i -> {
            logStorage.add(i, new ClientRegistrationEntry(i < 50 ? TERM_ONE : TERM_TWO, i));
        });
    }

    @Test
    void willThrowWhenTruncationBufferIsLessThanZero() {
        InMemorySnapshot snapshot = new InMemorySnapshot(55, TERM_TWO, LAST_CONFIG);
        assertThatThrownBy(() -> BufferedTruncationCalculator.calculateTruncation(snapshot, logStorage, -7))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Truncation buffer must be greater than zero (was -7)");

    }

    @Test
    void willCalculateTruncationInMiddleOfLog() {
        InMemorySnapshot snapshot = new InMemorySnapshot(55, TERM_TWO, LAST_CONFIG);
        final BufferedTruncationCalculator.TruncationDetails truncationDetails =
                BufferedTruncationCalculator.calculateTruncation(snapshot, logStorage, TRUNCATION_BUFFER);
        assertThat(truncationDetails).usingRecursiveComparison()
                .isEqualTo(new BufferedTruncationCalculator.TruncationDetails(45, TERM_ONE, 45));
    }

    @Test
    void willCalculateTruncationAfterEndOfLog() {
        InMemorySnapshot snapshot = new InMemorySnapshot(150, TERM_THREE, LAST_CONFIG);
        final BufferedTruncationCalculator.TruncationDetails truncationDetails =
                BufferedTruncationCalculator.calculateTruncation(snapshot, logStorage, TRUNCATION_BUFFER);
        assertThat(truncationDetails).usingRecursiveComparison()
                .isEqualTo(new BufferedTruncationCalculator.TruncationDetails(150, TERM_THREE, 100));
    }

    @Test
    void willTruncateEntireLogWhenTruncationBufferCannotBeFilled() {
        InMemorySnapshot snapshot = new InMemorySnapshot(105, TERM_THREE, LAST_CONFIG);
        final BufferedTruncationCalculator.TruncationDetails truncationDetails =
                BufferedTruncationCalculator.calculateTruncation(snapshot, logStorage, TRUNCATION_BUFFER);
        assertThat(truncationDetails).usingRecursiveComparison()
                .isEqualTo(new BufferedTruncationCalculator.TruncationDetails(105, TERM_THREE, 100));
    }

    @Test
    void willGenerateNoOpTruncationForSnapshotBeforeStartOfLog() {
        // install snapshot
        final InMemorySnapshot firstSnapshot = new InMemorySnapshot(30, TERM_ONE, LAST_CONFIG);
        logStorage.installSnapshot(firstSnapshot);
        assertThat(logStorage.getPrevIndex()).isEqualTo(20);
        assertThat(logStorage.getPrevTerm()).isEqualTo(TERM_ONE);

        // calculate truncation for earlier snapshot
        final InMemorySnapshot secondSnapshot = new InMemorySnapshot(15, TERM_ONE, LAST_CONFIG);
        final BufferedTruncationCalculator.TruncationDetails secondTruncationDetails =
                BufferedTruncationCalculator.calculateTruncation(secondSnapshot, logStorage, TRUNCATION_BUFFER);
        assertThat(secondTruncationDetails).usingRecursiveComparison()
                .isEqualTo(new BufferedTruncationCalculator.TruncationDetails(20, TERM_ONE, 0));
    }

    @Test
    void willUseSnapshotLastTermWhenTruncationBufferIsZero() {
        final InMemorySnapshot snapshot = new InMemorySnapshot(30, TERM_ONE, LAST_CONFIG);
        final InMemoryLogStorage logStorageSpy = spy(logStorage);
        final BufferedTruncationCalculator.TruncationDetails truncationDetails =
                BufferedTruncationCalculator.calculateTruncation(snapshot, logStorageSpy, 0);
        assertThat(truncationDetails).usingRecursiveComparison()
                .isEqualTo(new BufferedTruncationCalculator.TruncationDetails(30, TERM_ONE, 30));
        verify(logStorageSpy, never()).getEntry(anyInt());
    }
}