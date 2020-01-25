package au.id.tindall.distalg.raft.statemachine;

import au.id.tindall.distalg.raft.client.sessions.ClientSessionStore;
import au.id.tindall.distalg.raft.log.EntryCommittedEventHandler;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.entries.LogEntry;
import au.id.tindall.distalg.raft.log.entries.StateMachineCommandEntry;

import java.util.ArrayList;
import java.util.List;


public class CommandExecutor {

    private final StateMachine stateMachine;
    private final EntryCommittedEventHandler commandHandler;
    private final List<CommandAppliedEventHandler> commandAppliedEventHandlers;
    private final ClientSessionStore clientSessionStore;

    public CommandExecutor(StateMachine stateMachine, ClientSessionStore clientSessionStore) {
        this.stateMachine = stateMachine;
        this.clientSessionStore = clientSessionStore;
        this.commandAppliedEventHandlers = new ArrayList<>();
        this.commandHandler = this::handleStateMachineCommands;
        this.clientSessionStore.startListeningForAppliedCommands(this);
    }

    public void startListeningForCommittedCommands(Log log) {
        log.addEntryCommittedEventHandler(this.commandHandler);
    }

    public void stopListeningForCommittedCommands(Log log) {
        log.removeEntryCommittedEventHandler(this.commandHandler);
    }

    public void addCommandAppliedEventHandler(CommandAppliedEventHandler commandAppliedEventHandler) {
        this.commandAppliedEventHandlers.add(commandAppliedEventHandler);
    }

    public void removeCommandAppliedEventHandler(CommandAppliedEventHandler commandAppliedEventHandler) {
        this.commandAppliedEventHandlers.remove(commandAppliedEventHandler);
    }

    private void handleStateMachineCommands(int logIndex, LogEntry logEntry) {
        if (logEntry instanceof StateMachineCommandEntry) {
            StateMachineCommandEntry stateMachineCommandEntry = (StateMachineCommandEntry) logEntry;
            byte[] result = clientSessionStore.getCommandResult(stateMachineCommandEntry.getClientId(), stateMachineCommandEntry.getClientSequenceNumber())
                    .orElseGet(() -> this.stateMachine.apply(stateMachineCommandEntry.getCommand()));
            notifyCommandAppliedListeners(logIndex, stateMachineCommandEntry.getClientId(), stateMachineCommandEntry.getClientSequenceNumber(), result);
        }
    }

    private void notifyCommandAppliedListeners(int logIndex, int clientId, int sequenceNumber, byte[] result) {
        this.commandAppliedEventHandlers.forEach(handler -> handler.handleCommandApplied(logIndex, clientId, sequenceNumber, result));
    }
}
