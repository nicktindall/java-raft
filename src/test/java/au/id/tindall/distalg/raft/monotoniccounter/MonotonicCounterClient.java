package au.id.tindall.distalg.raft.monotoniccounter;

import au.id.tindall.distalg.raft.Server;
import au.id.tindall.distalg.raft.exceptions.NotRunningException;
import au.id.tindall.distalg.raft.rpc.client.ClientRequestMessage;
import au.id.tindall.distalg.raft.rpc.client.ClientRequestRequest;
import au.id.tindall.distalg.raft.rpc.client.ClientRequestResponse;
import au.id.tindall.distalg.raft.rpc.client.ClientRequestStatus;
import au.id.tindall.distalg.raft.rpc.client.ClientResponseMessage;
import au.id.tindall.distalg.raft.rpc.client.RegisterClientRequest;
import au.id.tindall.distalg.raft.rpc.client.RegisterClientResponse;
import au.id.tindall.distalg.raft.rpc.client.RegisterClientStatus;
import org.apache.logging.log4j.Logger;

import java.math.BigInteger;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import static au.id.tindall.distalg.raft.serverstates.ServerStateType.LEADER;
import static org.apache.logging.log4j.LogManager.getLogger;

/**
 * "client" for the Monotonic counter state machine
 * <p>
 * it increments the counter repeatedly checking that the current value is what it expects it to be
 */
public class MonotonicCounterClient {

    public static final int MAX_RETRIES = 20;
    private final Logger LOGGER = getLogger();

    private Integer clientId;
    private int clientSequenceNumber;
    private BigInteger counterValue = BigInteger.ZERO;
    private final Map<Long, Server<Long>> servers;

    public MonotonicCounterClient(Map<Long, Server<Long>> servers) {
        this.servers = servers;
    }

    public void register() throws ExecutionException, InterruptedException {
        int retries = 0;
        while (retries < MAX_RETRIES) {
            RegisterClientResponse<Long> response = (RegisterClientResponse<Long>) send(RegisterClientRequest::new).get();
            if (response.getStatus() == RegisterClientStatus.OK) {
                this.clientId = response.getClientId().get();
                return;
            } else {
                LOGGER.warn("Server responded with {}, retrying", response.getStatus());
                retries++;
                Thread.sleep(100L);
            }
        }
        throw new IllegalStateException("Couldn't register client!");
    }

    public void increment() throws ExecutionException, InterruptedException {
        int retries = 0;
        while (retries < MAX_RETRIES) {
            ClientRequestResponse<Long> commandResponse = (ClientRequestResponse<Long>) send(id -> new ClientRequestRequest<Long>(id, clientId, clientSequenceNumber, counterValue.toByteArray())).get();
            if (commandResponse.getStatus() == ClientRequestStatus.OK) {
                this.counterValue = new BigInteger(commandResponse.getResponse());
                clientSequenceNumber++;
                return;
            } else {
                LOGGER.warn("Server responded with status {}, retrying", commandResponse.getStatus());
                retries++;
            }
        }
        throw new RuntimeException("Maximum retries exceeded!");
    }

    private CompletableFuture<? extends ClientResponseMessage> send(Function<Long, ClientRequestMessage<Long>> request) {
        int retries = 0;
        while (retries < MAX_RETRIES) {
            Server<Long> leader = findLeader();
            try {
                return leader.handle(request.apply(leader.getId()));
            } catch (NotRunningException ex) {
                // Do nothing we'll retry
            }
            retries++;
        }
        throw new RuntimeException("Maximum retries exceeded, failing sending...");
    }

    private Server<Long> findLeader() {
        Optional<Server<Long>> leader = Optional.empty();
        while (leader.isEmpty()) {
            leader = servers.values().stream()
                    .filter(this::serverIsLeader)
                    .findAny();
        }
        return leader.get();
    }

    private boolean serverIsLeader(Server<Long> server) {
        return server.getState()
                .filter(state -> state == LEADER)
                .isPresent();
    }
}
