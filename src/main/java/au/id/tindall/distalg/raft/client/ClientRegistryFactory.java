package au.id.tindall.distalg.raft.client;

import java.io.Serializable;

public class ClientRegistryFactory<ID extends Serializable> {

    public ClientRegistry<ID> createClientRegistry() {
        return new ClientRegistry<>();
    }
}
