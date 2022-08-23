package au.id.tindall.distalg.raft.statemachine;

import au.id.tindall.distalg.raft.client.sessions.ClientSessionStore;
import au.id.tindall.distalg.raft.log.EntryAppendedEventHandler;
import au.id.tindall.distalg.raft.log.EntryCommittedEventHandler;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.entries.ConfigurationEntry;
import au.id.tindall.distalg.raft.log.entries.LogEntry;
import au.id.tindall.distalg.raft.log.entries.StateMachineCommandEntry;
import au.id.tindall.distalg.raft.snapshotting.Snapshotter;
import au.id.tindall.distalg.raft.state.Snapshot;
import au.id.tindall.distalg.raft.state.SnapshotInstalledListener;

import java.util.ArrayList;
import java.util.List;


public class CommandExecutor implements SnapshotInstalledListener {

    private final StateMachine stateMachine;
    private final EntryCommittedEventHandler entryCommittedEventHandler;
    private final List<CommandAppliedEventHandler> commandAppliedEventHandlers;
    private final ClientSessionStore clientSessionStore;
    private final Snapshotter snapshotter;
    private final EntryAppendedEventHandler entryAppendedEventHandler;
    private boolean creatingSnapshot = false;

    public CommandExecutor(StateMachine stateMachine, ClientSessionStore clientSessionStore, Snapshotter snapshotter) {
        this.stateMachine = stateMachine;
        this.clientSessionStore = clientSessionStore;
        this.commandAppliedEventHandlers = new ArrayList<>();
        this.entryCommittedEventHandler = this::handleStateMachineCommands;
        this.entryAppendedEventHandler = this::handleConfigurationEntries;
        this.clientSessionStore.startListeningForAppliedCommands(this);
        this.snapshotter = snapshotter;
    }

    public void startListeningForCommittedCommands(Log log) {
        log.addEntryCommittedEventHandler(this.entryCommittedEventHandler);
        log.addEntryAppendedEventHandler(this.entryAppendedEventHandler);
    }

    public void stopListeningForCommittedCommands(Log log) {
        log.removeEntryCommittedEventHandler(this.entryCommittedEventHandler);
        log.removeEntryAppendedEventHandler(this.entryAppendedEventHandler);
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
                    .orElseGet(() -> this.stateMachine.apply(logIndex, stateMachineCommandEntry.getCommand()));
            notifyCommandAppliedListeners(logIndex, stateMachineCommandEntry.getClientId(), stateMachineCommandEntry.lastResponseReceived(), stateMachineCommandEntry.getClientSequenceNumber(), result);
            try {
                creatingSnapshot = true;
                snapshotter.createSnapshotIfReady(logIndex, logEntry.getTerm());
            } finally {
                creatingSnapshot = false;
            }
        }
    }

    private void handleConfigurationEntries(int logIndex, LogEntry logEntry) {
        if (logEntry instanceof ConfigurationEntry) {
            snapshotter.logConfigurationEntry((ConfigurationEntry) logEntry);
        }
    }

    private void notifyCommandAppliedListeners(int logIndex, int clientId, int lastResponseReceived, int sequenceNumber, byte[] result) {
        this.commandAppliedEventHandlers.forEach(handler -> handler.handleCommandApplied(logIndex, clientId, lastResponseReceived, sequenceNumber, result));
    }

    @Override
    public void onSnapshotInstalled(Snapshot snapshot) {
        if (!creatingSnapshot) {
            stateMachine.installSnapshot(snapshot);
        }
    }
}
