package au.id.tindall.distalg.raft.util;

public enum HexUtil {
    ;

    public static String hexDump(byte[] bytes, int offset, int length) {
        StringBuilder builder = new StringBuilder();
        for (int i = offset; i < bytes.length && i - offset < length; i++) {
            builder.append(String.format("%02X ", bytes[i]));
        }
        if (builder.length() > 0) {
            builder.setLength(builder.length() - 1);
        }
        return builder.toString();
    }

    public static String hexDump(byte[] bytes, int length) {
        return hexDump(bytes, 0, length);
    }

    public static String hexDump(byte[] bytes) {
        return hexDump(bytes, bytes.length);
    }
}
