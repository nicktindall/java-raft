package au.id.tindall.distalg.raft.serverstates;

public class Result<I> {

    private final boolean finished;
    private final ServerState<I> nextState;

    public static <I> Result<I> complete(ServerState<I> serverState) {
        return new Result<>(true, serverState);
    }

    public static <I> Result<I> incomplete(ServerState<I> serverState) {
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
