package au.id.tindall.distalg.raft.rpc.client;

import java.util.Arrays;

public class ClientRequestResponse extends ClientResponseMessage {

    private final ClientRequestStatus status;
    private final byte[] response;
    private final Integer leaderHint;

    public ClientRequestResponse(ClientRequestStatus status, byte[] response, Integer leaderHint) {
        this.status = status;
        this.response = Arrays.copyOf(response, response.length);
        this.leaderHint = leaderHint;
    }

    public ClientRequestStatus getStatus() {
        return status;
    }

    public byte[] getResponse() {
        return Arrays.copyOf(response, response.length);
    }

    public Integer getLeaderHint() {
        return leaderHint;
    }
}
