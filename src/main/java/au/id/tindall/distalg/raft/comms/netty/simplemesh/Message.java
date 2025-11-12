package au.id.tindall.distalg.raft.comms.netty.simplemesh;

import au.id.tindall.distalg.raft.serialisation.Streamable;

public record Message(int source, Streamable message) {
}
