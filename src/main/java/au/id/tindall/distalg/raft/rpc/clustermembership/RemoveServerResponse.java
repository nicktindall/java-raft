package au.id.tindall.distalg.raft.rpc.clustermembership;

public class RemoveServerResponse extends ClusterMembershipResponse {

    public enum Status {
        OK,
        NOT_LEADER,
        TIMEOUT
    }

    private final Status status;

    public RemoveServerResponse(Status status) {
        this.status = status;
    }
}
