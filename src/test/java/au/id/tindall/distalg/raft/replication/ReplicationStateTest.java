package au.id.tindall.distalg.raft.replication;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class ReplicationStateTest {

    public static final int INITIAL_NEXT_INDEX = 456;
    public static final long FOLLOWER_ID = 123L;

    private ReplicationState<Long> replicationState;

    @BeforeEach
    void setUp() {
        replicationState = new ReplicationState<>(FOLLOWER_ID, INITIAL_NEXT_INDEX);
    }

    @Nested
    class Constructor {
        @Test
        void matchIndexWillBeInitializedToZero() {
            assertThat(replicationState.getMatchIndex()).isZero();
        }

        @Test
        void nextIndexWillBeInitializedToValuePassed() {
            assertThat(replicationState.getNextIndex()).isEqualTo(INITIAL_NEXT_INDEX);
        }
    }

    @Nested
    class LogSuccessResponse {

        @Test
        void willSetNextIndex() {
            int lastAppendedIndex = INITIAL_NEXT_INDEX + 1;
            replicationState.logSuccessResponse(lastAppendedIndex);
            assertThat(replicationState.getNextIndex()).isEqualTo(lastAppendedIndex + 1);
        }

        @Test
        void willSetMatchIndex() {
            int lastAppendedIndex = 2;
            replicationState.logSuccessResponse(lastAppendedIndex);
            assertThat(replicationState.getMatchIndex()).isEqualTo(lastAppendedIndex);
        }

        @Test
        void willOnlyAdvanceIndices() {
            int lastAppendedIndex = INITIAL_NEXT_INDEX + 1;
            replicationState.logSuccessResponse(lastAppendedIndex);
            assertThat(replicationState.getMatchIndex()).isEqualTo(lastAppendedIndex);
            assertThat(replicationState.getNextIndex()).isEqualTo(lastAppendedIndex + 1);

            replicationState.logSuccessResponse(lastAppendedIndex - 1);
            assertThat(replicationState.getMatchIndex()).isEqualTo(lastAppendedIndex);
            assertThat(replicationState.getNextIndex()).isEqualTo(lastAppendedIndex + 1);
        }
    }

    @Nested
    class LogFailedResponse {

        @Test
        void willNotChangeNextIndexWhenEarliestPossibleMatchIsNotSpecified() {
            replicationState.logFailedResponse(null);
            assertThat(replicationState.getNextIndex()).isEqualTo(INITIAL_NEXT_INDEX);
        }

        @Test
        void willSetNextIndexWhenEarliestPossibleMatchIsSpecified() {
            replicationState.logFailedResponse(20);
            assertThat(replicationState.getNextIndex()).isEqualTo(20);
        }

        @Test
        void willNotModifyMatchIndex() {
            replicationState.logFailedResponse(null);
            assertThat(replicationState.getMatchIndex()).isZero();
        }
    }

}