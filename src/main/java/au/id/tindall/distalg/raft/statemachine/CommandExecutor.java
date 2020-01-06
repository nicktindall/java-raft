package au.id.tindall.distalg.raft.statemachine;

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

    public CommandExecutor(StateMachine stateMachine) {
        this.stateMachine = stateMachine;
        this.commandAppliedEventHandlers = new ArrayList<>();
        this.commandHandler = this::handleStateMachineCommands;
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

    private void handleStateMachineCommands(int index, LogEntry logEntry) {
        if (logEntry instanceof StateMachineCommandEntry) {
            StateMachineCommandEntry stateMachineCommandEntry = (StateMachineCommandEntry) logEntry;
            byte[] result = this.stateMachine.apply(stateMachineCommandEntry.getCommand());
            notifyListeners(index, result);
        }
    }

    private void notifyListeners(int index, byte[] result) {
        this.commandAppliedEventHandlers.forEach(handler -> handler.handleCommandApplied(index, result));
    }
}
