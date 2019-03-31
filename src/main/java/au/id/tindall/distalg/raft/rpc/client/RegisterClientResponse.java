package au.id.tindall.distalg.raft.rpc.client;

import java.io.Serializable;
import java.util.Optional;

public class RegisterClientResponse<ID extends Serializable> extends ClientResponseMessage {

    private final RegisterClientStatus status;
    private final Integer clientId;
    private final ID leaderHint;

    public RegisterClientResponse(RegisterClientStatus status, Integer clientId, ID leaderHint) {
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

    public Optional<ID> getLeaderHint() {
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
