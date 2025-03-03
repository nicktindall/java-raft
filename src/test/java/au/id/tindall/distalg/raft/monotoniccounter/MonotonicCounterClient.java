package au.id.tindall.distalg.raft.monotoniccounter;

import au.id.tindall.distalg.raft.Server;
import au.id.tindall.distalg.raft.comms.AbstractClusterClient;
import au.id.tindall.distalg.raft.comms.ConnectionClosedException;
import au.id.tindall.distalg.raft.rpc.client.ClientRequestRequest;
import au.id.tindall.distalg.raft.rpc.client.ClientRequestResponse;
import au.id.tindall.distalg.raft.rpc.client.ClientRequestStatus;
import au.id.tindall.distalg.raft.rpc.client.RegisterClientRequest;
import au.id.tindall.distalg.raft.rpc.client.RegisterClientResponse;
import au.id.tindall.distalg.raft.rpc.client.RegisterClientStatus;
import org.apache.logging.log4j.Logger;

import java.math.BigInteger;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static au.id.tindall.distalg.raft.util.ThreadUtil.pauseMillis;
import static org.apache.logging.log4j.LogManager.getLogger;

/**
 * "client" for the Monotonic counter state machine
 * <p>
 * it increments the counter repeatedly checking that the current value is what it expects it to be
 */
public class MonotonicCounterClient extends AbstractClusterClient {

    public static final int MAX_RETRIES = 20;
    private final Logger LOGGER = getLogger();

    private Integer clientId;
    private int clientSequenceNumber;
    private BigInteger counterValue;

    public MonotonicCounterClient(Map<Long, Server<Long>> servers, BigInteger startingValue) {
        super(servers);
        this.counterValue = startingValue;
    }

    public void register() throws ExecutionException, InterruptedException {
        int retries = 0;
        while (retries < MAX_RETRIES) {
            try {
                RegisterClientResponse<Long> response = sendClientRequest(RegisterClientRequest::new).get(1, TimeUnit.SECONDS);
                if (response.getStatus() == RegisterClientStatus.OK) {
                    this.clientId = response.getClientId().get();
                    return;
                } else {
                    LOGGER.debug("Server responded with {}, retrying", response.getStatus());

                }
            } catch (ExecutionException e) {
                if (e.getCause() instanceof ConnectionClosedException) {
                    LOGGER.debug("Connection was closed, retrying");
                } else {
                    throw e;
                }
            } catch (TimeoutException e) {
                LOGGER.debug("No response received, retrying");
            }
            retries++;
            pauseMillis(100);
        }
        throw new IllegalStateException("Couldn't register client!");
    }

    public void increment(Runnable failureChecker) throws ExecutionException, InterruptedException {
        int retries = 0;
        while (retries < MAX_RETRIES) {
            failureChecker.run();
            try {
                ClientRequestResponse<Long> commandResponse = sendClientRequest(id -> new ClientRequestRequest<>(id, clientId, clientSequenceNumber, clientSequenceNumber - 1, counterValue.toByteArray())).get(1, TimeUnit.SECONDS);
                if (commandResponse.getStatus() == ClientRequestStatus.OK) {
                    this.counterValue = new BigInteger(commandResponse.getResponse());
                    clientSequenceNumber++;
                    return;
                } else {
                    LOGGER.debug("Server responded with status {}, retrying (counterValue={})", commandResponse.getStatus(), counterValue);
                }
            } catch (ExecutionException e) {
                if (e.getCause() instanceof ConnectionClosedException) {
                    LOGGER.debug("Connection was closed, retrying");
                } else {
                    throw e;
                }
            } catch (TimeoutException e) {
                LOGGER.debug("No response received, retrying");
            }
            retries++;
            pauseMillis(100L);
        }
        throw new IllegalStateException("Maximum retries exceeded!");
    }
}
