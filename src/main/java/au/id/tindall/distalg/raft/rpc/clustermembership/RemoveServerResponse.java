package au.id.tindall.distalg.raft.rpc.clustermembership;

import au.id.tindall.distalg.raft.rpc.client.ClientResponseMessage;

public enum RemoveServerResponse implements ClientResponseMessage {
    OK(Status.OK),
    NOT_LEADER(Status.NOT_LEADER);

    public enum Status {
        OK,
        NOT_LEADER,
        TIMEOUT
    }

    private final Status status;

    RemoveServerResponse(Status status) {
        this.status = status;
    }

    public Status getStatus() {
        return status;
    }
}
