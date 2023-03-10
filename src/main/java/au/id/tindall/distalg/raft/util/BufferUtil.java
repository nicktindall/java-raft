package au.id.tindall.distalg.raft.util;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.nio.ByteBuffer;

public enum BufferUtil {
    ;

    private static final MethodHandle INVOKE_CLEANER;

    static {
        MethodHandles.Lookup lookup = MethodHandles.lookup();
        try {
            INVOKE_CLEANER = lookup.findVirtual(UnsafeUtil.get().getClass(), "invokeCleaner", MethodType.methodType(void.class, ByteBuffer.class));
        } catch (NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalStateException("Couldn't get invokeCleaner", e);
        }
    }

    /**
     * Free a ByteBuffer
     *
     * @param byteBuffer
     */
    public static void free(ByteBuffer byteBuffer) {
        if (!byteBuffer.isDirect()) {
            return;
        }
        try {
            INVOKE_CLEANER.invokeExact(UnsafeUtil.get(), byteBuffer);
        } catch (Throwable t) {
            throw new IllegalStateException("Error freeing byte buffer", t);
        }
    }
}
