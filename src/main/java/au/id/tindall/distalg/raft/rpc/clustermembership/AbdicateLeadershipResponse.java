package au.id.tindall.distalg.raft.rpc.clustermembership;

import au.id.tindall.distalg.raft.rpc.client.ClientResponseMessage;

public class AbdicateLeadershipResponse<I> implements ClientResponseMessage<I> {

    @SuppressWarnings("rawtypes")
    private static final AbdicateLeadershipResponse OK = new AbdicateLeadershipResponse(Status.OK);
    @SuppressWarnings("rawtypes")
    private static final AbdicateLeadershipResponse NOT_LEADER = new AbdicateLeadershipResponse(Status.NOT_LEADER);

    @SuppressWarnings("unchecked")
    public static <I> AbdicateLeadershipResponse<I> getOK() {
        return OK;
    }

    @SuppressWarnings("unchecked")
    public static <I> AbdicateLeadershipResponse<I> getNotLeader() {
        return NOT_LEADER;
    }

    public AbdicateLeadershipResponse(Status status) {
        this.status = status;
    }

    public enum Status {
        OK,
        NOT_LEADER
    }

    private final Status status;

    @Override
    public boolean isFromLeader() {
        return status != Status.NOT_LEADER;
    }
}
