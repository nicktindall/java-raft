package au.id.tindall.distalg.raft.rpc.clustermembership;

public class RemoveServerResponse extends ClusterMembershipResponse {

    public enum Status {
        OK,
        NOT_LEADER,
        TIMEOUT
    }

    public static RemoveServerResponse OK = new RemoveServerResponse(Status.OK);
    public static RemoveServerResponse NOT_LEADER = new RemoveServerResponse(Status.NOT_LEADER);

    private final Status status;

    private RemoveServerResponse(Status status) {
        this.status = status;
    }
}
