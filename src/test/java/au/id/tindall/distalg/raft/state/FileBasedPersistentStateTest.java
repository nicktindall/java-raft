package au.id.tindall.distalg.raft.state;

import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.storage.LogStorage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.File;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(MockitoExtension.class)
class FileBasedPersistentStateTest {

    private static final long SERVER_ID = 123L;
    private static final long VOTED_FOR = 456L;
    private static final Term CURRENT_TERM = new Term(9);
    private File tempFile;
    @Mock
    private LogStorage logStorage;

    @BeforeEach
    void setUp() throws IOException {
        tempFile = File.createTempFile("logFileTest", "append");
        tempFile.delete();
    }

    @AfterEach
    void tearDown() {
        tempFile.delete();
    }

    @Test
    void willRestoreFromFile() {
        PersistentState<Long> original = new FileBasedPersistentState<>(logStorage, tempFile.toPath(), new JavaIDSerializer<>(), SERVER_ID);
        original.setCurrentTerm(CURRENT_TERM);
        original.setVotedFor(VOTED_FOR);
        PersistentState<Long> restored = new FileBasedPersistentState<>(logStorage, tempFile.toPath(), new JavaIDSerializer<>());
        assertThat(restored.getId()).isEqualTo(SERVER_ID);
        assertThat(restored.getCurrentTerm()).isEqualTo(CURRENT_TERM);
        assertThat(restored.getVotedFor()).contains(VOTED_FOR);
    }

    @Nested
    class SetCurrentTerm {

        private FileBasedPersistentState<Long> persistentState;

        @BeforeEach
        void setUp() {
            persistentState = new FileBasedPersistentState<>(logStorage, tempFile.toPath(), new JavaIDSerializer<>(), SERVER_ID);
        }

        @Test
        void willClearVotedForOnAdvance() {
            persistentState.setVotedFor(VOTED_FOR);
            persistentState.setCurrentTerm(CURRENT_TERM);
            assertThat(persistentState.getVotedFor()).isEmpty();
        }

        @Test
        void willNotClearVotedForOnNoChange() {
            persistentState.setVotedFor(VOTED_FOR);
            persistentState.setCurrentTerm(persistentState.getCurrentTerm());
            assertThat(persistentState.getVotedFor()).contains(VOTED_FOR);
        }

        @Test
        void willThrowWhenTermReduces() {
            assertThatThrownBy(() -> {
                persistentState.setCurrentTerm(CURRENT_TERM.next());
                persistentState.setCurrentTerm(CURRENT_TERM);
            }).isInstanceOf(IllegalArgumentException.class);
        }
    }
}