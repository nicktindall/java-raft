package au.id.tindall.distalg.raft.serialisation;

import au.id.tindall.distalg.raft.util.RandomTestUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
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

class FileChannelIOTest {

    private static final int CHUNK_SIZE_BYTES = 10240;
    private FileChannelIO fileChannelIO;

    @BeforeEach
    void setUp(@TempDir Path tempDir) throws IOException {
        Path testFile = tempDir.resolve("test_file");
        fileChannelIO = new FileChannelIO(StringIDSerializer.INSTANCE,
                FileChannel.open(testFile, StandardOpenOption.CREATE_NEW, StandardOpenOption.READ, StandardOpenOption.WRITE),
                FileChannel.MapMode.READ_WRITE,
                CHUNK_SIZE_BYTES);
    }

    @Test
    void writeThenReadInt() {
        final int value = ThreadLocalRandom.current().nextInt();
        fileChannelIO.writeInteger(value);
        assertThat(fileChannelIO.readInteger()).isEqualTo(value);
    }

    @Test
    void writeThenReadString() {
        final String value = UUID.randomUUID().toString();
        fileChannelIO.writeString(value);
        assertThat(fileChannelIO.readString()).isEqualTo(value);
    }

    @Test
    void writeThenReadDouble() {
        final double value = ThreadLocalRandom.current().nextDouble();
        fileChannelIO.writeDouble(value);
        assertThat(fileChannelIO.readDouble()).isEqualTo(value);
    }

    @Test
    void writeThenReadBytes() {
        final byte[] value = UUID.randomUUID().toString().getBytes();
        fileChannelIO.writeBytes(value);
        assertThat(fileChannelIO.readBytes()).isEqualTo(value);
    }

    @Test
    void writeThenReadIdentifier() {
        final String value = UUID.randomUUID().toString();
        fileChannelIO.writeIdentifier(value);
        assertThat((String) fileChannelIO.readIdentifier()).isEqualTo(value);
    }

    @Test
    void writeThenReadBoolean() {
        final boolean value = ThreadLocalRandom.current().nextBoolean();
        fileChannelIO.writeBoolean(value);
        assertThat(fileChannelIO.readBoolean()).isEqualTo(value);
    }

    @Test
    void writeThenReadLong() {
        final long value = ThreadLocalRandom.current().nextLong();
        fileChannelIO.writeLong(value);
        assertThat(fileChannelIO.readLong()).isEqualTo(value);
    }

    @Test
    void writeThenReadList() {
        final List<Long> values = IntStream.range(0, 30).mapToLong(ignored -> ThreadLocalRandom.current().nextLong()).boxed().toList();
        fileChannelIO.writeList(values, StreamingOutput::writeLong);
        assertThat(fileChannelIO.readList(ArrayList::new, StreamingInput::readLong)).isEqualTo(values);
    }

    @Test
    void writeThenReadNullable() {
        final String value = ThreadLocalRandom.current().nextBoolean() ? randomStringValue(20) : null;
        fileChannelIO.writeNullable(value, StreamingOutput::writeString);
        assertThat(fileChannelIO.readNullable(StreamingInput::readString)).isEqualTo(value);
    }

    @Test
    void writeThenReadSet() {
        final Set<String> value = randomSet(ThreadLocalRandom.current().nextInt(100), r -> randomStringValue(r, 20));
        fileChannelIO.writeSet(value, StreamingOutput::writeString);
        assertThat(fileChannelIO.readSet(HashSet::newHashSet, StreamingInput::readString))
                .isEqualTo(value);
    }

    @Test
    void writeThenReadMap() {
        final Map<String, Integer> value = randomMap(ThreadLocalRandom.current().nextInt(100), r -> randomStringValue(16), RandomTestUtil::randomIntValue);
        fileChannelIO.writeMap(value, StreamingOutput::writeString, StreamingOutput::writeInteger);
        assertThat(fileChannelIO.readMap(size -> new HashMap<>(), StreamingInput::readString, StreamingInput::readInteger))
                .isEqualTo(value);
    }

    @Test
    void writeThenReadRandomChunks() {
        final int seed = ThreadLocalRandom.current().nextInt();
        final Random random = new Random(seed);
        for (int i = 0; i < 100; i++) {
            int writeSize = random.nextInt(0, CHUNK_SIZE_BYTES);
            byte[] bytes = randomByteArray(random, writeSize);
            fileChannelIO.writeBytes(bytes);
            assertThat(fileChannelIO.readBytes()).isEqualTo(bytes);
        }
    }
}