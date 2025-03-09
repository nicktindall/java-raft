package au.id.tindall.distalg.raft.rpc.clustermembership;

import au.id.tindall.distalg.raft.rpc.client.ClientResponseMessage;

public enum AbdicateLeadershipResponse implements ClientResponseMessage {
    OK,
    NOT_LEADER
}
