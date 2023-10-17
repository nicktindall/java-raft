package au.id.tindall.distalg.raft.rpc.clustermembership;

public enum AddServerResponse implements ServerAdminResponse {
    OK(Status.OK),
    TIMEOUT(Status.TIMEOUT),
    NOT_LEADER(Status.NOT_LEADER);

    public enum Status {
        OK,
        NOT_LEADER,
        TIMEOUT
    }

    AddServerResponse(Status status) {
        this.status = status;
    }

    private final Status status;

    public Status getStatus() {
        return status;
    }

    @Override
    public String toString() {
        return "AddServerResponse{" +
                "status=" + status +
                '}';
    }
}
