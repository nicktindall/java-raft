package au.id.tindall.distalg.raft.statemachine;

public class CommandExecutorFactory {

    public CommandExecutor createCommandExecutor(StateMachine stateMachine) {
        return new CommandExecutor(stateMachine);
    }
}
