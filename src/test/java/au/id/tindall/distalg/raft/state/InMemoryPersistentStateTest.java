package au.id.tindall.distalg.raft.state;

import au.id.tindall.distalg.raft.log.Term;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class InMemoryPersistentStateTest {

    private static final long SERVER_ID = 1234L;
    private static final long OTHER_SERVER_ID = 789L;

    private InMemoryPersistentState<Long> persistentState;

    @BeforeEach
    void setUp() {
        persistentState = new InMemoryPersistentState<>(SERVER_ID);
    }

    @Nested
    class SetCurrentTerm {

        @Test
        void willThrow_WhenTermIsLessThanCurrentTerm() {
            persistentState.setCurrentTerm(new Term(10));
            assertThatThrownBy(() -> persistentState.setCurrentTerm(new Term(6)))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void willClearVotedFor_WhenTermIsAdvanced() {
            persistentState.setVotedFor(OTHER_SERVER_ID);
            persistentState.setCurrentTerm(new Term(10));
            assertThat(persistentState.getVotedFor()).isEmpty();
        }

        @Test
        void willNotClearVotedFor_WhenTermIsNotAdvanced() {
            persistentState.setCurrentTerm(new Term(10));
            persistentState.setVotedFor(OTHER_SERVER_ID);
            persistentState.setCurrentTerm(new Term(10));
            assertThat(persistentState.getVotedFor()).contains(OTHER_SERVER_ID);
        }
    }
}