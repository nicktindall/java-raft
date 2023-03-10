package au.id.tindall.distalg.raft.util;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

public enum UnsafeUtil {
    ;

    private static final Unsafe instance;

    static {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            instance = (Unsafe) f.get(null);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new IllegalStateException("Couldn't get a reference to unsafe", e);
        }
    }

    public static Unsafe get() {
        return instance;
    }
}
