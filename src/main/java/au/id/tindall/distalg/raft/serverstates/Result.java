package au.id.tindall.distalg.raft.serverstates;

import java.io.Serializable;

public class Result<ID extends Serializable> {

    private final boolean finished;
    private final ServerState<ID> nextState;

    public static <ID extends Serializable> Result<ID> complete(ServerState<ID> serverState) {
        return new Result<>(true, serverState);
    }

    public static <ID extends Serializable> Result<ID> incomplete(ServerState<ID> serverState) {
        return new Result<>(false, serverState);
    }

    public Result(boolean finished, ServerState<ID> nextState) {
        this.finished = finished;
        this.nextState = nextState;
    }

    public boolean isFinished() {
        return finished;
    }

    public ServerState<ID> getNextState() {
        return nextState;
    }
}
