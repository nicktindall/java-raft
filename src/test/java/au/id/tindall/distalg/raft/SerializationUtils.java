package au.id.tindall.distalg.raft;

import static au.id.tindall.distalg.raft.util.SerializationUtil.deserializeObject;
import static au.id.tindall.distalg.raft.util.SerializationUtil.serializeObject;

public class SerializationUtils {

    @SuppressWarnings("unchecked")
    public static <T> T roundTripSerializeDeserialize(T object) {
        return deserializeObject(serializeObject(object));
    }
}
