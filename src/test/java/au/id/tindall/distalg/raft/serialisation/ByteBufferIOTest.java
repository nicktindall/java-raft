package au.id.tindall.distalg.raft.serialisation;

import au.id.tindall.distalg.raft.util.RandomTestUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import static au.id.tindall.distalg.raft.util.RandomTestUtil.randomByteArray;
import static au.id.tindall.distalg.raft.util.RandomTestUtil.randomMap;
import static au.id.tindall.distalg.raft.util.RandomTestUtil.randomSet;
import static au.id.tindall.distalg.raft.util.RandomTestUtil.randomStringValue;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;


class ByteBufferIOTest {

    private ByteBufferIO streamingIO;

    @BeforeEach
    void setUp() {
        streamingIO = ByteBufferIO.fixed(new TestIDSerializer(), 10240);
    }

    @Test
    void writeThenReadInt() {
        final int value = ThreadLocalRandom.current().nextInt();
        streamingIO.writeInteger(value);
        assertThat(streamingIO.readInteger()).isEqualTo(value);
    }

    @Test
    void writeThenReadString() {
        final String value = UUID.randomUUID().toString();
        streamingIO.writeString(value);
        assertThat(streamingIO.readString()).isEqualTo(value);
    }

    @Test
    void writeThenReadDouble() {
        final double value = ThreadLocalRandom.current().nextDouble();
        streamingIO.writeDouble(value);
        assertThat(streamingIO.readDouble()).isEqualTo(value);
    }

    @Test
    void writeThenReadBytes() {
        final byte[] value = UUID.randomUUID().toString().getBytes();
        streamingIO.writeBytes(value);
        assertThat(streamingIO.readBytes()).isEqualTo(value);
    }

    @Test
    void writeThenReadIdentifier() {
        final String value = UUID.randomUUID().toString();
        streamingIO.writeIdentifier(value);
        assertThat((String) streamingIO.readIdentifier()).isEqualTo(value);
    }

    @Test
    void writeThenReadBoolean() {
        final boolean value = ThreadLocalRandom.current().nextBoolean();
        streamingIO.writeBoolean(value);
        assertThat(streamingIO.readBoolean()).isEqualTo(value);
    }

    @Test
    void writeThenReadLong() {
        final long value = ThreadLocalRandom.current().nextLong();
        streamingIO.writeLong(value);
        assertThat(streamingIO.readLong()).isEqualTo(value);
    }

    @Test
    void writeThenReadList() {
        final List<Long> values = IntStream.range(0, 30).mapToLong(ignored -> ThreadLocalRandom.current().nextLong()).boxed().toList();
        streamingIO.writeList(values, StreamingOutput::writeLong);
        assertThat(streamingIO.readList(ArrayList::new, StreamingInput::readLong)).isEqualTo(values);
    }

    @Test
    void writeThenReadNullable() {
        final String value = ThreadLocalRandom.current().nextBoolean() ? randomStringValue(20) : null;
        streamingIO.writeNullable(value, StreamingOutput::writeString);
        assertThat(streamingIO.readNullable(StreamingInput::readString)).isEqualTo(value);
    }

    @Test
    void writeThenReadSet() {
        final Set<String> value = randomSet(ThreadLocalRandom.current().nextInt(100), r -> randomStringValue(r, 20));
        streamingIO.writeSet(value, StreamingOutput::writeString);
        assertThat(streamingIO.readSet(HashSet::newHashSet, StreamingInput::readString))
                .isEqualTo(value);
    }

    @Test
    void writeThenReadMap() {
        final Map<String, Integer> value = randomMap(ThreadLocalRandom.current().nextInt(100), r -> randomStringValue(16), RandomTestUtil::randomIntValue);
        streamingIO.writeMap(value, StreamingOutput::writeString, StreamingOutput::writeInteger);
        assertThat(streamingIO.readMap(size -> new HashMap<>(), StreamingInput::readString, StreamingInput::readInteger))
                .isEqualTo(value);
    }

    @Test
    void elasticBuffer() {
        final int seed = ThreadLocalRandom.current().nextInt();
        final ByteBufferIO bbio = ByteBufferIO.elastic(UnsupportedIDSerializer.INSTANCE);
        final Random random = new Random(seed);
        writeRandomInteractions(random, bbio);
        random.setSeed(seed);
        readRandomInteractions(random, bbio);
    }

    private void writeRandomInteractions(Random random, ByteBufferIO bbio) {
        final int numberOfOps = random.nextInt(1000, 5000);
        for (int i = 0; i < numberOfOps; i++) {
            switch (random.nextInt(7)) {
                case 0 -> bbio.writeInteger(random.nextInt());
                case 1 -> bbio.writeLong(random.nextLong());
                case 2 -> bbio.writeBoolean(random.nextBoolean());
                case 3 -> bbio.writeDouble(random.nextDouble());
                case 4 -> {
                    final byte[] array = new byte[random.nextInt(300)];
                    random.nextBytes(array);
                    bbio.writeBytes(array);
                }
                case 5 -> bbio.writeMap(
                        createRandomMap(random),
                        StreamingOutput::writeString,
                        StreamingOutput::writeInteger);
                case 6 -> bbio.writeList(
                        createRandomList(random),
                        StreamingOutput::writeString);
                default -> fail("Op not implemented");
            }
        }
    }

    private List<String> createRandomList(Random random) {
        return IntStream.range(0, random.nextInt(1000))
                .mapToObj(ignored -> randomStringValue(random, random.nextInt(30)))
                .toList();
    }

    private static Map<String, Integer> createRandomMap(Random random) {
        return randomMap(
                random,
                random.nextInt(50),
                r -> randomStringValue(r, 10),
                RandomTestUtil::randomIntValue);
    }

    private void readRandomInteractions(Random random, ByteBufferIO bbio) {
        final int numberOfOps = random.nextInt(100, 500);
        for (int i = 0; i < numberOfOps; i++) {
            switch (random.nextInt(7)) {
                case 0 -> assertEquals(random.nextInt(), bbio.readInteger());
                case 1 -> assertEquals(random.nextLong(), bbio.readLong());
                case 2 -> assertEquals(random.nextBoolean(), bbio.readBoolean());
                case 3 -> assertEquals(random.nextDouble(), bbio.readDouble());
                case 4 -> {
                    final byte[] array = new byte[random.nextInt(300)];
                    random.nextBytes(array);
                    assertArrayEquals(array, bbio.readBytes());
                }
                case 5 -> assertEquals(
                        createRandomMap(random),
                        bbio.readMap(HashMap::new, StreamingInput::readString, StreamingInput::readInteger));
                case 6 ->
                        assertEquals(createRandomList(random), bbio.readList(ArrayList::new, StreamingInput::readString));
                default -> fail("OP not implemented");
            }
        }
    }

    @Test
    void testContentAsBytes() {
        final List<byte[]> allContents = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            final byte[] chunk = randomByteArray();
            allContents.add(chunk);
            streamingIO.writeBytes(chunk);
        }
        final ByteBuffer contentAsBytes = ByteBuffer.wrap(streamingIO.contentAsBytes());
        for (byte[] chunk : allContents) {
            assertThat(contentAsBytes.getInt()).isEqualTo(chunk.length);
            byte[] readChunk = new byte[chunk.length];
            contentAsBytes.get(readChunk);
            assertThat(readChunk).isEqualTo(chunk);
        }
        assertThat(contentAsBytes.remaining()).isZero();
    }

    private static class TestIDSerializer implements IDSerializer {

        @Override
        public void writeId(Object id, StreamingOutput streamingOutput) {
            streamingOutput.writeString(id.toString());
        }

        @Override
        public <T> T readId(StreamingInput streamingInput) {
            return (T) streamingInput.readString();
        }
    }
}