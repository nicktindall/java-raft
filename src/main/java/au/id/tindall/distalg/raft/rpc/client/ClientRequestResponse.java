package au.id.tindall.distalg.raft.rpc.client;

import java.io.Serializable;
import java.util.Arrays;

public class ClientRequestResponse<I extends Serializable> implements ClientResponseMessage {

    private final ClientRequestStatus status;
    private final byte[] response;
    private final I leaderHint;

    public ClientRequestResponse(ClientRequestStatus status, byte[] response, I leaderHint) {
        this.status = status;
        this.response = response != null ? Arrays.copyOf(response, response.length) : null;
        this.leaderHint = leaderHint;
    }

    public ClientRequestStatus getStatus() {
        return status;
    }

    public byte[] getResponse() {
        return Arrays.copyOf(response, response.length);
    }

    public I getLeaderHint() {
        return leaderHint;
    }
}
