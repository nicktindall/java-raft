package au.id.tindall.distalg.raft;

import au.id.tindall.distalg.raft.serialisation.ByteBufferIO;
import au.id.tindall.distalg.raft.serialisation.IDSerializer;
import au.id.tindall.distalg.raft.serialisation.Streamable;

public class SerializationUtils {

    public static <T extends Streamable> T roundTripSerializeDeserialize(T object, IDSerializer idSerializer) {
        final ByteBufferIO bbio = ByteBufferIO.elastic(idSerializer);
        bbio.writeStreamable(object);
        return bbio.readStreamable();
    }
}
