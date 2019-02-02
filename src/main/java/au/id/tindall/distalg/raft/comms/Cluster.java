package au.id.tindall.distalg.raft.comms;

import java.io.Serializable;
import java.util.Set;

public interface Cluster<ID extends Serializable> {

    void sendMessage(ID destination, Object message);

    void broadcastMessage(Object message);

    boolean isQuorum(Set<ID> receivedVotes);
}
