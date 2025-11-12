package au.id.tindall.distalg.raft.comms.netty.simplemesh.messages;

import au.id.tindall.distalg.raft.SerializationUtils;
import au.id.tindall.distalg.raft.serialisation.MessageIdentifier;
import au.id.tindall.distalg.raft.serialisation.Streamable;
import au.id.tindall.distalg.raft.serialisation.StreamingInput;
import au.id.tindall.distalg.raft.serialisation.StreamingOutput;
import au.id.tindall.distalg.raft.serialisation.UnsupportedIDSerializer;
import org.junit.jupiter.api.Test;

import static au.id.tindall.distalg.raft.util.RandomTestUtil.randomIntValue;
import static org.assertj.core.api.Assertions.assertThat;

class PayloadMessageTest {

    @Test
    void canSerializeAndDeserialize() {
        PayloadMessage payloadMessage = new PayloadMessage(new Contents(randomIntValue()));
        assertThat(SerializationUtils.roundTripSerializeDeserialize(payloadMessage, UnsupportedIDSerializer.INSTANCE))
                .usingRecursiveComparison()
                .isEqualTo(payloadMessage);
    }

    public static class Contents implements Streamable {

        private static final MessageIdentifier MESSAGE_IDENTIFIER = MessageIdentifier.registerMessageIdentifier("PayloadMessage.test.Contents", Contents.class);

        private final int value;

        public Contents(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }

        @SuppressWarnings("unused")
        public Contents(StreamingInput streamingInput) {
            this.value = streamingInput.readInteger();
        }

        @Override
        public MessageIdentifier getMessageIdentifier() {
            return MESSAGE_IDENTIFIER;
        }

        @Override
        public void writeTo(StreamingOutput streamingOutput) {
            streamingOutput.writeInteger(value);
        }
    }
}