package au.id.tindall.distalg.raft.comms;

public interface MessageDispatcher<ID> {

    void sendMessage(ID destination, Object message);

    void broadcastMessage(Object message);
}
