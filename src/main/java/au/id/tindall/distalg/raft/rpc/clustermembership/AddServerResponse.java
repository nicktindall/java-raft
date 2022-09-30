package au.id.tindall.distalg.raft.rpc.clustermembership;

public class AddServerResponse extends ClusterMembershipResponse {

    public enum Status {
        OK,
        NOT_LEADER,
        TIMEOUT
    }

    public static final AddServerResponse OK = new AddServerResponse(Status.OK);
    public static final AddServerResponse TIMEOUT = new AddServerResponse(Status.TIMEOUT);
    public static final AddServerResponse NOT_LEADER = new AddServerResponse(Status.NOT_LEADER);

    private AddServerResponse(Status status) {
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
