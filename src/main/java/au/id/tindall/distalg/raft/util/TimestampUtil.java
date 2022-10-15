package au.id.tindall.distalg.raft.util;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public enum TimestampUtil {
    ;

    public static String formatTimestamp(long timestampMs) {
        return formatTimestamp(Instant.ofEpochMilli(timestampMs));
    }

    public static String formatTimestamp(Instant timestamp) {
        return DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(timestamp.atZone(ZoneId.systemDefault()));
    }
}
