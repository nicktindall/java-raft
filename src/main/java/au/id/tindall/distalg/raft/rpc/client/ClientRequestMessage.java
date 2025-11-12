package au.id.tindall.distalg.raft.rpc.client;

import au.id.tindall.distalg.raft.serialisation.Streamable;

@SuppressWarnings("unused")
public interface ClientRequestMessage<I, R extends ClientResponseMessage<I>> extends Streamable {
}
