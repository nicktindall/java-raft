package au.id.tindall.distalg.raft.util;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;

public enum RandomTestUtil {
    ;

    public static <E extends Enum<E>> E randomEnum(Class<E> enumClass) {
        final E[] enumConstants = enumClass.getEnumConstants();
        final int ordinal = ThreadLocalRandom.current().nextInt(enumConstants.length);
        return enumConstants[ordinal];
    }

    public static byte[] randomByteArray(Random random, int length) {
        final byte[] bytes = new byte[length];
        random.nextBytes(bytes);
        return bytes;
    }

    public static byte[] randomByteArray() {
        final int length = ThreadLocalRandom.current().nextInt(4096);
        return randomByteArray(ThreadLocalRandom.current(), length);
    }

    public static int randomIntValue() {
        return randomIntValue(ThreadLocalRandom.current());
    }

    public static int randomIntValue(Random random) {
        return random.nextInt(10000);
    }

    public static boolean randomBoolean() {
        return ThreadLocalRandom.current().nextBoolean();
    }

    public static String randomStringValue(int length) {
        return randomStringValue(ThreadLocalRandom.current(), length);
    }

    public static String randomStringValue(Random random, int length) {
        final StringBuilder b = new StringBuilder();
        for (int i = 0; i < length; i++) {
            b.append((char) random.nextInt('A', 'z'));
        }
        return b.toString();
    }

    public static <T> Set<T> randomSet(int size, Function<Random, T> randomGenerator) {
        return randomSet(ThreadLocalRandom.current(), size, randomGenerator);
    }

    public static <T> Set<T> randomSet(Random random, int size, Function<Random, T> valueGenerator) {
        Set<T> set = new HashSet<>();
        int counter = 0;
        while (counter < size) {
            final T value = valueGenerator.apply(random);
            if (set.add(value)) {
                counter++;
            }
        }
        return set;
    }

    public static <K, V> Map<K, V> randomMap(int size, Function<Random, K> keyGenerator, Function<Random, V> valueGenerator) {
        return randomMap(ThreadLocalRandom.current(), size, keyGenerator, valueGenerator);
    }

    public static <K, V> Map<K, V> randomMap(Random random, int size, Function<Random, K> keyGenerator, Function<Random, V> valueGenerator) {
        Map<K, V> map = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            while (map.put(keyGenerator.apply(random), valueGenerator.apply(random)) != null) {
                // Try again on collisions
            }
        }
        return map;
    }
}
