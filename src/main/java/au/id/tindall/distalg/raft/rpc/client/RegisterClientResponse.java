package au.id.tindall.distalg.raft.rpc.client;

import java.io.Serializable;
import java.util.Optional;

public class RegisterClientResponse<I extends Serializable> implements ClientResponseMessage {

    private final RegisterClientStatus status;
    private final Integer clientId;
    private final I leaderHint;

    public RegisterClientResponse(RegisterClientStatus status, Integer clientId, I leaderHint) {
        this.status = status;
        this.clientId = clientId;
        this.leaderHint = leaderHint;
    }

    public RegisterClientStatus getStatus() {
        return status;
    }

    public Optional<Integer> getClientId() {
        return Optional.ofNullable(clientId);
    }

    public Optional<I> getLeaderHint() {
        return Optional.ofNullable(leaderHint);
    }

    @Override
    public String toString() {
        return "RegisterClientResponse{" +
                "status=" + status +
                ", clientId=" + clientId +
                ", leaderHint=" + leaderHint +
                "} " + super.toString();
    }
}
