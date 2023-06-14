package au.id.tindall.distalg.raft.serverstates;

import java.io.Serializable;

public class Result<I extends Serializable> {

    private final boolean finished;
    private final ServerState<I> nextState;

    public static <I extends Serializable> Result<I> complete(ServerState<I> serverState) {
        return new Result<>(true, serverState);
    }

    public static <I extends Serializable> Result<I> incomplete(ServerState<I> serverState) {
        return new Result<>(false, serverState);
    }

    public Result(boolean finished, ServerState<I> nextState) {
        this.finished = finished;
        this.nextState = nextState;
    }

    public boolean isFinished() {
        return finished;
    }

    public ServerState<I> getNextState() {
        return nextState;
    }

    @Override
    public String toString() {
        return "Result{" +
                "finished=" + finished +
                ", nextState=" + nextState +
                '}';
    }
}
