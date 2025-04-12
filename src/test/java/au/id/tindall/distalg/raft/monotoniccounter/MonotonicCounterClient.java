package au.id.tindall.distalg.raft.monotoniccounter;

import au.id.tindall.distalg.raft.clusterclient.AbstractClusterClient;
import au.id.tindall.distalg.raft.clusterclient.ClusterClient;
import au.id.tindall.distalg.raft.rpc.client.ClientRequestRequest;
import au.id.tindall.distalg.raft.rpc.client.ClientRequestResponse;
import au.id.tindall.distalg.raft.rpc.client.ClientRequestStatus;
import au.id.tindall.distalg.raft.rpc.client.RegisterClientRequest;
import au.id.tindall.distalg.raft.rpc.client.RegisterClientResponse;
import au.id.tindall.distalg.raft.rpc.client.RegisterClientStatus;

import java.math.BigInteger;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.fail;

/**
 * "client" for the Monotonic counter state machine
 * <p>
 * it increments the counter repeatedly checking that the current value is what it expects it to be
 */
public class MonotonicCounterClient extends AbstractClusterClient<Long> {

    private Integer clientId;
    private int clientSequenceNumber;
    private BigInteger counterValue;

    public MonotonicCounterClient(ClusterClient<Long> clusterClient, BigInteger startingValue) {
        super(clusterClient);
        this.counterValue = startingValue;
    }

    public void register() throws InterruptedException {
        try {
            RegisterClientResponse<Long> response = sendClientRequest(new RegisterClientRequest<>(), 10_000).get();
            if (response.getStatus() == RegisterClientStatus.OK) {
                this.clientId = response.getClientId().get();
            } else {
                fail(String.format("Server responded with %s, failing", response.getStatus()));
            }
        } catch (ExecutionException e) {
            fail(e);
        }
    }

    public void increment(Runnable failureChecker) throws InterruptedException {
        failureChecker.run();
        try {
            ClientRequestResponse<Long> commandResponse = sendClientRequest(new ClientRequestRequest<>(clientId, clientSequenceNumber, clientSequenceNumber - 1, counterValue.toByteArray()), 10_000).get();
            if (commandResponse.getStatus() == ClientRequestStatus.OK) {
                this.counterValue = new BigInteger(commandResponse.getResponse());
                clientSequenceNumber++;
            } else {
                fail(String.format("Server responded with status %s, retrying (counterValue=%s)", commandResponse.getStatus(), counterValue));
            }
        } catch (ExecutionException e) {
            fail(e);
        }
    }
}
