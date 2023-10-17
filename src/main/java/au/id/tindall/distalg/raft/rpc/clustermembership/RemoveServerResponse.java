package au.id.tindall.distalg.raft.rpc.clustermembership;

public enum RemoveServerResponse implements ServerAdminResponse {
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
