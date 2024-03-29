package au.id.tindall.distalg.raft.statemachine;

import au.id.tindall.distalg.raft.client.sessions.ClientSessionStore;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.StateMachineCommandEntry;
import au.id.tindall.distalg.raft.snapshotting.Snapshotter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;

import static java.util.Collections.singletonList;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CommandExecutorTest {

    private static final Term TERM_0 = new Term(0);
    private static final int CLIENT_ID = 123;
    private static final int CLIENT_SEQUENCE_NUMBER = 456;
    private static final byte[] COMMAND = "hello".getBytes();
    private static final byte[] RESULT = "goodbye".getBytes();

    @Mock
    private StateMachine stateMachine;
    @Mock
    private ClientSessionStore clientSessionStore;
    @Mock
    private Snapshotter snapshotter;

    private CommandExecutor commandExecutor;

    @BeforeEach
    void setUp() {
        commandExecutor = new CommandExecutor(stateMachine, clientSessionStore, snapshotter);
    }

    @Test
    void shouldStartNotifyingClientSessionStoreOfAppliedCommands() {
        verify(clientSessionStore).startListeningForAppliedCommands(commandExecutor);
    }

    @Nested
    class WhenStateMachineCommandEntryCommitted {

        @Mock
        private CommandAppliedEventHandler commandAppliedEventHandler;
        private Log log;

        @BeforeEach
        void setUp() {
            log = new Log();
            commandExecutor.startListeningForCommittedCommands(log);
            commandExecutor.addCommandAppliedEventHandler(commandAppliedEventHandler);
            log.appendEntries(0, singletonList(new StateMachineCommandEntry(TERM_0, CLIENT_ID, -1, CLIENT_SEQUENCE_NUMBER, COMMAND)));
        }

        @Nested
        class AndCommandIsDuplicate {

            @BeforeEach
            void setUp() {
                when(clientSessionStore.getCommandResult(CLIENT_ID, CLIENT_SEQUENCE_NUMBER)).thenReturn(Optional.of(RESULT));
            }

            @Test
            void shouldNotApplyCommandToStateMachine() {
                when(clientSessionStore.getCommandResult(CLIENT_ID, CLIENT_SEQUENCE_NUMBER)).thenReturn(Optional.of(RESULT));
                log.advanceCommitIndex(1);
                verifyNoMoreInteractions(stateMachine);
            }

            @Test
            void shouldNotifyListenersOfCommandResult_WhenCommandIsDuplicate() {
                when(clientSessionStore.getCommandResult(CLIENT_ID, CLIENT_SEQUENCE_NUMBER)).thenReturn(Optional.of(RESULT));
                log.advanceCommitIndex(1);
                verify(commandAppliedEventHandler).handleCommandApplied(1, CLIENT_ID, -1, CLIENT_SEQUENCE_NUMBER, RESULT);
            }
        }

        @Nested
        class AndCommandIsNotDuplicate {

            @Test
            void shouldApplyCommandToStateMachine() {
                log.advanceCommitIndex(1);
                verify(stateMachine).apply(1, COMMAND);
            }

            @Test
            void shouldNotifyListenersOfCommandResult() {
                when(stateMachine.apply(1, COMMAND)).thenReturn(RESULT);
                log.advanceCommitIndex(1);
                verify(commandAppliedEventHandler).handleCommandApplied(1, CLIENT_ID, -1, CLIENT_SEQUENCE_NUMBER, RESULT);
            }
        }

        @Test
        void shouldNotNotifyRemovedListenersOfCommandResult() {
            commandExecutor.removeCommandAppliedEventHandler(commandAppliedEventHandler);
            log.advanceCommitIndex(1);
            verifyNoMoreInteractions(commandAppliedEventHandler);
        }

        @Test
        void shouldNotBeNotifiedAfterWeStopListening() {
            commandExecutor.stopListeningForCommittedCommands(log);
            log.advanceCommitIndex(1);
            verifyNoMoreInteractions(stateMachine);
            verifyNoMoreInteractions(commandAppliedEventHandler);
        }
    }
}