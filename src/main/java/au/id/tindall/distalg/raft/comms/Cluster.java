package au.id.tindall.distalg.raft.comms;

import java.util.Set;

public interface Cluster<ID> {

    void sendMessage(ID destination, Object message);

    void broadcastMessage(Object message);

    boolean isQuorum(Set<ID> receivedVotes);
}
